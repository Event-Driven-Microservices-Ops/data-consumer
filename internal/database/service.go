package database

import (
	"context"
	"os"

	"github.com/urbaniakmichal/data-consumer/internal/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type DataBaseService struct {
	cfg              *config.Config
	mongoClient      *mongo.Client
	streamCollection *mongo.Collection
	batchCollection  *mongo.Collection
}

func NewDataBaseService(cfg *config.Config, mC *mongo.Client) *DataBaseService {
	sC, bC := createCollections(cfg, mC)
	return &DataBaseService{
		cfg:              cfg,
		mongoClient:      mC,
		streamCollection: sC,
		batchCollection:  bC,
	}
}

func createCollections(cfg *config.Config, mC *mongo.Client) (*mongo.Collection, *mongo.Collection) {
	if envDBName := os.Getenv("DB_NAME"); envDBName != "" {
		cfg.DatabaseName = envDBName
	}
	if envStreamColl := os.Getenv("STREAM_COLLECTION"); envStreamColl != "" {
		cfg.StreamCollection = envStreamColl
	}
	if envBatchColl := os.Getenv("BATCH_COLLECTION"); envBatchColl != "" {
		cfg.BatchCollection = envBatchColl
	}

	db := mC.Database(cfg.DatabaseName)
	streamCollection := db.Collection(cfg.StreamCollection)
	batchCollection := db.Collection(cfg.BatchCollection)

	return streamCollection, batchCollection
}

func (ds *DataBaseService) InsertBatch(ctx context.Context, documents []interface{}) error {
	_, err := ds.batchCollection.InsertMany(ctx, documents)
	return err
}

func (ds *DataBaseService) InsertStream(ctx context.Context, document interface{}) error {
	_, err := ds.streamCollection.InsertOne(ctx, document)
	return err
}
