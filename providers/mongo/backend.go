package mongo

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/okharch/cachecalc/v4/distlock"
	"github.com/okharch/cachecalc/v4/valuestore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type valueDoc struct {
	Key       string    `bson:"_id"`
	Data      []byte    `bson:"data"`
	ExpireAt  time.Time `bson:"expire_at"`
}

type lockDoc struct {
	Key      string    `bson:"_id"`
	Token    []byte    `bson:"token"`
	ExpireAt time.Time `bson:"expire_at"`
}

// Backend provides both shared value storage and distributed locks on MongoDB.
type Backend struct {
	client *mongo.Client
	values *mongo.Collection
	locks  *mongo.Collection
}

func New(ctx context.Context, uri string) (*Backend, error) {
	if uri == "" {
		uri = os.Getenv("MONGO_URL")
	}
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017"
	}
	clientOpts := options.Client().ApplyURI(uri).
		SetConnectTimeout(5 * time.Second).
		SetServerSelectionTimeout(5 * time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping mongo: %w", err)
	}
	db := client.Database("smartcache")
	values := db.Collection("values")
	locks := db.Collection("locks")

	// TTL index for automatic expiry cleanup.
	ttlIdx := mongo.IndexModel{
		Keys:    bson.D{{Key: "expire_at", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	}
	if _, err := values.Indexes().CreateOne(ctx, ttlIdx); err != nil {
		return nil, fmt.Errorf("create values TTL index: %w", err)
	}
	if _, err := locks.Indexes().CreateOne(ctx, ttlIdx); err != nil {
		return nil, fmt.Errorf("create locks TTL index: %w", err)
	}

	return &Backend{client: client, values: values, locks: locks}, nil
}

func (b *Backend) Get(ctx context.Context, key string) (valuestore.EntrySnapshot, bool, error) {
	var doc valueDoc
	err := b.values.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return valuestore.EntrySnapshot{}, false, nil
	}
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	entry, err := valuestore.Unmarshal(doc.Data)
	if err != nil {
		return valuestore.EntrySnapshot{}, false, err
	}
	if !entry.Usable(time.Now()) {
		_ = b.Delete(ctx, key)
		return valuestore.EntrySnapshot{}, false, nil
	}
	return entry, true, nil
}

func (b *Backend) Put(ctx context.Context, key string, entry valuestore.EntrySnapshot) error {
	buf, err := valuestore.Marshal(entry)
	if err != nil {
		return err
	}
	doc := valueDoc{Key: key, Data: buf, ExpireAt: entry.ExpireAt}
	opts := options.Replace().SetUpsert(true)
	_, err = b.values.ReplaceOne(ctx, bson.M{"_id": key}, doc, opts)
	return err
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.values.DeleteOne(ctx, bson.M{"_id": key})
	return err
}

func (b *Backend) TryAcquire(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	now := time.Now()
	expireAt := now.Add(lockTTL)

	// Delete any expired lock first so the insert can succeed.
	_, _ = b.locks.DeleteOne(ctx, bson.M{"_id": key, "expire_at": bson.M{"$lte": now}})

	doc := lockDoc{Key: key, Token: token, ExpireAt: expireAt}
	_, err := b.locks.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *Backend) Renew(ctx context.Context, key string, token []byte, lockTTL time.Duration) (bool, error) {
	expireAt := time.Now().Add(lockTTL)
	res, err := b.locks.UpdateOne(ctx,
		bson.M{"_id": key, "token": token},
		bson.M{"$set": bson.M{"expire_at": expireAt}},
	)
	if err != nil {
		return false, err
	}
	return res.MatchedCount == 1, nil
}

func (b *Backend) Release(ctx context.Context, key string, token []byte) (bool, error) {
	res, err := b.locks.DeleteOne(ctx, bson.M{"_id": key, "token": token})
	if err != nil {
		return false, err
	}
	return res.DeletedCount == 1, nil
}

func (b *Backend) LockProvider() distlock.Provider {
	return distlock.NewProvider(b)
}

func (b *Backend) Close() error {
	return b.client.Disconnect(context.Background())
}
