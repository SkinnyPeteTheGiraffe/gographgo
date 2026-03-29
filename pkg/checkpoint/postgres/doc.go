// Package postgres provides a Postgres-backed checkpoint saver.
//
// Production recommendation: manage schema migrations outside the application
// (for example, in CI/CD) and construct savers with AutoMigrate disabled.
//
//	saver, err := postgres.OpenWithOptions(dsn, checkpoint.JSONSerializer{}, postgres.Options{})
//
// If the schema is not present, OpenWithOptions/NewWithOptions return a clear
// error instructing callers to run Saver.Migrate or enable AutoMigrate.
//
// To explicitly run built-in migrations:
//
//	saver, err := postgres.OpenWithOptions(dsn, checkpoint.JSONSerializer{}, postgres.Options{})
//	if err != nil {
//	    return err
//	}
//	if err := saver.Migrate(ctx); err != nil {
//	    return err
//	}
//
// To opt into one-step migration during construction:
//
//	saver, err := postgres.OpenWithOptions(
//	    dsn,
//	    checkpoint.JSONSerializer{},
//	    postgres.Options{AutoMigrate: true},
//	)
//
// For compatibility with older behavior, OpenAutoMigrate and NewAutoMigrate
// are provided.
package postgres
