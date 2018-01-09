package orm

import (
	"database/sql"

	"github.com/go-xorm/xorm"
)

type DB struct {
	Engine  *xorm.Engine
	Session *xorm.Session
}

// func (db *DB) WithSession(sess *xorm.Session) *DB {
// 	return &DB{Engine: db.Engine, session: sess}
// }
//
// func (db *DB) Begin() (*DB, error) {
// 	if db.session != nil {
// 		return nil, errors.New("run in the transaction")
// 	}
// 	session := db.Engine.NewSession()
//  if err := session.Begin(); err != nil {
// 	  return nil, err
//  }
// 	return &DB{Engine: db.Engine, session: session}, nil
// }

func (db *DB) From(from *DB) *DB {
	db.Engine = from.Engine
	db.Session = from.Session
	return db
}

func (db *DB) Commit() error {
	if db.Session == nil {
		return sql.ErrTxDone
	}
	err := db.Session.Commit()
	db.Session = nil
	return err
}

// func (db *DB) Tx() *sql.Tx {
// 	if db.Session == nil {
// 		return nil
// 	}
// 	return db.Session.Tx()
// }

func (db *DB) Rollback() error {
	if db.Session == nil {
		return sql.ErrTxDone
	}
	err := db.Session.Rollback()
	db.Session = nil
	return err
}

func (db *DB) Close() error {
	if db.Session != nil {
		db.Session.Close()
	}
	return nil
}

func (db *DB) Exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	return NewWithNoInstance()(db.Engine).
		WithSession(db.Session).Exec(sqlStr, args...)
}

func (db *DB) Query(sqlStr string, args ...interface{}) Queryer {
	return NewWithNoInstance()(db.Engine).
		WithSession(db.Session).
		Query(sqlStr, args...)
}
