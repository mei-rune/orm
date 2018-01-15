package orm

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/go-xorm/builder"
	"github.com/go-xorm/xorm"
	"github.com/lib/pq"
)

// ErrNotFound record isn't found
var ErrNotFound = errors.New("record isn't found")

// ErrInsertFail record is create fail
var ErrInsertFail = errors.New("create record fail")

// ValidationError store the Message & Key of a validation error
type ValidationError struct {
	Code, Message, Key string
}

// Error store a error with validation errors
type Error struct {
	Validations []ValidationError
	e           error
}

func (err *Error) Error() string {
	return err.e.Error()
}

func toError(e error, keyFor func(string) string) error {
	if e == nil {
		return nil
	}

	if pe, ok := e.(*pq.Error); ok {
		switch pe.Code {
		case "23505":
			detail := strings.TrimPrefix(strings.TrimPrefix(pe.Detail, "Key ("), "键值\"(")
			if pidx := strings.Index(detail, ")"); pidx > 0 {
				return &Error{Validations: []ValidationError{
					{Code: "unique_value_already_exists", Message: pe.Detail, Key: keyFor(detail[:pidx])},
				}, e: e}
			}
		// case "23503":
		// 	return &Error{Validations: []ValidationError{
		// 		{Code: "PG.foreign_key_constraint", Message: pe.Message},
		// 	}, e: e}
		default:

			return &Error{Validations: []ValidationError{
				{Code: "PG." + pe.Code.Name(), Message: pe.Message, Key: keyFor(pe.Column)},
			}, e: e}
		}
	}
	return e
}

type Tx struct {
	Clean   func(sess *xorm.Session)
	Session *xorm.Session
}

func (tx *Tx) Commit() error {
	if tx.Clean != nil {
		tx.Clean(tx.Session)
		tx.Clean = nil
	}
	return tx.Session.Commit()
}

func (tx *Tx) Rollback() error {
	if tx.Clean != nil {
		tx.Clean(tx.Session)
		tx.Clean = nil
	}
	return tx.Session.Rollback()
}

func (tx *Tx) Close() error {
	if tx.Clean != nil {
		tx.Clean(tx.Session)
		tx.Clean = nil
	}
	return tx.Session.Rollback()
}

// Collection is an interface that defines methods useful for handling tables.
type Collection struct {
	Engine    *xorm.Engine
	session   *xorm.Session
	instance  func() interface{}
	keyFor    func(string) string
	tableName string
}

func New(instance func() interface{}, keyFor func(string) string) func(engine *xorm.Engine) *Collection {
	return func(engine *xorm.Engine) *Collection {
		tableName := engine.TableInfo(instance()).Name
		return &Collection{Engine: engine, instance: instance, keyFor: keyFor, tableName: tableName}
	}
}

func NewWithNoInstance() func(engine *xorm.Engine) *Collection {
	return func(engine *xorm.Engine) *Collection {
		return &Collection{Engine: engine, keyFor: keyForNull}
	}
}

func keyForNull(s string) string {
	return s
}

func (collection *Collection) WithSession(sess *xorm.Session) *Collection {
	if collection.session != nil {
		panic(errors.New("collection is running in the transaction"))
	}

	copyed := collection.copy()
	copyed.session = sess
	return copyed
}

func (collection *Collection) copy() *Collection {
	return &Collection{
		Engine:    collection.Engine,
		session:   collection.session,
		instance:  collection.instance,
		keyFor:    collection.keyFor,
		tableName: collection.tableName,
	}
}

func (collection *Collection) Begin() (*Tx, error) {
	if collection.session != nil {
		return nil, errors.New("collection is running in the transaction")
	}
	collection.session = collection.Engine.NewSession()
	return &Tx{Clean: func(sess *xorm.Session) {
		if sess != nil {
			if collection.session != sess {
				panic("不支持并发事务")
			}
		}
		collection.session = nil
	}, Session: collection.session}, nil
}

func (collection *Collection) table(bean interface{}) *xorm.Session {
	if collection.session == nil {
		return collection.Engine.Table(bean)
	}
	return collection.session.Table(bean)
}

func (collection *Collection) query(sql string, args ...interface{}) *xorm.Session {
	if collection.session == nil {
		return collection.Engine.SQL(sql, args...)
	}
	return collection.session.SQL(sql, args...)
}

func (collection *Collection) Exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	if collection.session == nil {
		return collection.Engine.Exec(sqlStr, args...)
	}
	return collection.session.Exec(sqlStr, args...)
}

type Inserter interface {
	// Nullable set null when column is zero-value and nullable for insert
	Nullable(columns ...string) Inserter

	// Insert record
	Insert(bean interface{}) (interface{}, error)
}

// Queryer
type Queryer interface {
	// One fetches the first result within the result set and dumps it into the
	// given pointer to struct or pointer to map. The result set is automatically
	// closed after picking the element, so there is no need to call Close()
	// after using One().
	One(ptrToStruct interface{}) error

	// All fetches all results within the result set and dumps them into the
	// given pointer to slice of maps or structs.  The result set is
	// automatically closed, so there is no need to call Close() after
	// using All().
	All(beans interface{}) error
}

// Nullable set null when column is zero-value and nullable for insert
func (collection *Collection) Nullable(columns ...string) Inserter {
	copyed := collection.copy()
	if copyed.session == nil {
		copyed.session = copyed.Engine.NewSession().Nullable(columns...)
	} else {
		copyed.session = copyed.session.Nullable(columns...)
	}
	return copyed
}

// Omit set null when column is zero-value and nullable for insert
func (collection *Collection) Omit(columns ...string) *Collection {
	copyed := collection.copy()
	if copyed.session == nil {
		copyed.session = copyed.Engine.NewSession().Omit(columns...)
	} else {
		copyed.session = copyed.session.Omit(columns...)
	}
	return copyed
}

// Insert inserts a new item into the collection, it accepts one argument
// that can be either a map or a struct. If the call suceeds, it returns the
// ID of the newly added element as an `interface{}` (the underlying type of
// this ID is unknown and depends on the database adapter). The ID returned
// by Insert() could be passed directly to Find() to retrieve the newly added
// element.
func (collection *Collection) Insert(bean interface{}) (interface{}, error) {
	rowsAffected, err := collection.table(collection.tableName).
		InsertOne(bean)
	if err != nil {
		return nil, toError(err, collection.keyFor)
	}
	if rowsAffected == 0 {
		return nil, ErrInsertFail
	}
	table := collection.Engine.TableInfo(bean)
	autoIncrColumn := table.AutoIncrColumn()
	if autoIncrColumn == nil {
		return nil, nil
	}
	aiValue, err := autoIncrColumn.ValueOf(bean)
	if err != nil {
		return nil, err
	}
	return aiValue.Interface(), nil
}

// Exists returns true if the collection exists, false otherwise.
func (collection *Collection) Exists() (bool, error) {
	return collection.Engine.IsTableExist(collection.tableName)
}

// Find defines a new result set with elements from the collection.
func (collection *Collection) Where(args ...interface{}) *Result {
	result := &Result{&QueryResult{collection: collection,
		session:  collection.table(collection.tableName),
		instance: collection.instance}}
	if len(args) != 0 {
		if len(args) == 1 {
			if c, ok := args[0].(Cond); ok {
				return result.And(c)
			}
		}

		sqlStr, ok := args[0].(string)
		if !ok {
			panic(errors.New("Where() 参数不正确，只能是一个 orm.Cond 对象，或一个字符串加多个参数"))
		}

		result.session = result.session.And(builder.Expr(sqlStr, args[1:]...))
	}
	return result
}

func (collection *Collection) Query(sqlStr string, args ...interface{}) *RawResult {
	session := collection.query(sqlStr, args...)
	result := &RawResult{RawQueryResult: RawQueryResult{session: session},
		instance: collection.instance}
	return result
}

// Name returns the name of the collection.
func (collection *Collection) Name() string {
	return collection.tableName
}

func (collection *Collection) Id(id interface{}) *IDResult {
	return collection.ID(id)
}

// Id provides converting id as a query condition
func (collection *Collection) ID(id interface{}) *IDResult {
	return &IDResult{collection: collection,
		session:  collection.table(collection.tableName),
		instance: collection.instance,
		id:       id}
}

// Result is an interface that defines methods useful for working with result
// sets.
type Result struct {
	*QueryResult
}

// Where discards all the previously set filtering constraints (if any) and
// sets new ones. Commonly used when the conditions of the result depend on
// external parameters that are yet to be evaluated:
//
//   res := col.Find()
//
//   if ... {
//     res.Where(...)
//   } else {
//     res.Where(...)
//   }
func (result *Result) Where(cond Cond) *Result {
	return result.And(cond)
}

// Or adds more filtering conditions on top of the existing constraints.
func (result *Result) Or(cond Cond) *Result {
	if len(cond) != 0 {
		result.session = result.session.Or(toConds(cond, nil))
	}
	return result
}

// And adds more filtering conditions on top of the existing constraints.
func (result *Result) And(cond Cond) *Result {
	if len(cond) != 0 {
		result.session = result.session.And(toConds(cond, nil))
	}
	return result
}

// Delete deletes all items within the result set. `Offset()` and `Limit()` are
// not honoured by `Delete()`.
func (result *Result) Delete() (int64, error) {
	rowEffected, err := result.session.Delete(result.instance())
	return rowEffected, toError(err, result.collection.keyFor)
}

// Update modifies all items within the result set. `Offset()` and `Limit()`
// are not honoured by `Update()`.
func (result *Result) Update(columns map[string]interface{}) (int64, error) {
	rowEffected, err := result.session.Update(columns)
	return rowEffected, toError(err, result.collection.keyFor)
}

// Count returns the number of items that match the set conditions. `Offset()`
// and `Limit()` are not honoured by `Count()`
func (result *Result) Count() (int64, error) {
	count, err := result.session.Count(result.instance())
	return count, toError(err, result.collection.keyFor)
}

// QueryResult is an interface that defines methods useful for working with result
// sets.
type QueryResult struct {
	limit, offset int
	collection    *Collection
	session       *xorm.Session
	instance      func() interface{}
}

// Limit defines the maximum number of results in this set. It only has
// effect on `One()`, `All()` and `Next()`.
func (result *QueryResult) Limit(limit int) *QueryResult {
	result.limit = limit
	result.session = result.session.Limit(limit, result.offset)
	return result
}

// Offset ignores the first *n* results. It only has effect on `One()`, `All()`
// and `Next()`.
func (result *QueryResult) Offset(offset int) *QueryResult {
	result.offset = offset
	result.session = result.session.Limit(result.limit, offset)
	return result
}

// OrderBy receives field names that define the order in which elements will be
// returned in a query
func (result *QueryResult) OrderBy(orders ...string) *QueryResult {
	if len(orders) > 0 {
		result.session = result.session.OrderBy(strings.Join(orders, ","))
	}
	return result
}

// Desc provide desc order by query condition, the input parameters are columns.
func (result *QueryResult) Desc(colNames ...string) *QueryResult {
	result.session = result.session.Desc(colNames...)
	return result
}

// Asc provide asc order by query condition, the input parameters are columns.
func (result *QueryResult) Asc(colNames ...string) *QueryResult {
	result.session = result.session.Asc(colNames...)
	return result
}

// Columns only use the parameters as select columns
func (result *QueryResult) Columns(colNames ...string) *QueryResult {
	result.session = result.session.Cols(colNames...)
	return result
}

// Omit only not use the parameters as select or update columns
func (result *QueryResult) Omit(columns ...string) *QueryResult {
	result.session = result.session.Omit(columns...)
	return result
}

// GroupBy is used to group results that have the same value in the same column
// or columns.
func (result *QueryResult) GroupBy(keys ...string) *QueryResult {
	result.session = result.session.GroupBy(strings.Join(keys, ","))
	return result
}

// Having Generate Having statement
func (result *QueryResult) Having(conditions string) *QueryResult {
	result.session = result.session.Having(conditions)
	return result
}

// One fetches the first result within the result set and dumps it into the
// given pointer to struct or pointer to map. The result set is automatically
// closed after picking the element, so there is no need to call Close()
// after using One().
func (result *QueryResult) One(ptrToStruct interface{}) error {
	found, err := result.session.Get(ptrToStruct)
	if err != nil {
		return toError(err, result.collection.keyFor)
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// All fetches all results within the result set and dumps them into the
// given pointer to slice of maps or structs.  The result set is
// automatically closed, so there is no need to call Close() after
// using All().
func (result *QueryResult) All(beans interface{}) error {
	return result.session.Find(beans)
}

// ForEach record by record handle records from table, condiBeans's non-empty fields
// are conditions. beans could be []Struct, []*Struct, map[int64]Struct
// map[int64]*Struct
func (result *QueryResult) ForEach(cb func(i int, read func(bean interface{}) error) error) error {
	return result.session.Iterate(result.instance(), func(i int, instance interface{}) error {
		return cb(i, func(bean interface{}) error {
			return copyStruct(bean, instance)
		})
	})
}

// IDResult is an interface that defines methods useful for working with result
// sets.
type IDResult struct {
	collection *Collection
	session    *xorm.Session
	instance   func() interface{}
	id         interface{}
}

// Get get one item by id.
func (result *IDResult) Get(bean interface{}) error {
	found, err := result.session.Id(result.id).Get(bean)
	if err != nil {
		return toError(err, result.collection.keyFor)
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// Delete deletes one item by id.
func (result *IDResult) Delete() error {
	rowsAffected, err := result.session.Id(result.id).Delete(result.instance())
	if err != nil {
		return toError(err, result.collection.keyFor)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// Update modifies one item by id.
func (result *IDResult) Update(bean interface{}, isAllCols ...bool) error {
	session := result.session.Id(result.id)
	if len(isAllCols) == 0 || isAllCols[0] {
		session = session.AllCols()
	}

	rowsAffected, err := session.Update(bean)
	if err != nil {
		if result.collection == nil {
			return toError(err, keyForNull)
		}
		return toError(err, result.collection.keyFor)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// Omit only not use the parameters as select or update columns
func (result *IDResult) Omit(columns ...string) ByID {
	result.session = result.session.Omit(columns...)
	return result
}

// Nullable set null when column is zero-value and nullable for update
func (result *IDResult) Nullable(columns ...string) Updater {
	result.session = result.session.Nullable(columns...)
	return result
}

// Columns only use the parameters as update columns
func (result *IDResult) Columns(columns ...string) ByID {
	result.session = result.session.Cols(columns...)
	return result
}

type Updater interface {
	// Nullable set null when column is zero-value and nullable for update
	Nullable(columns ...string) Updater
	// Update records
	Update(bean interface{}, isAllCols ...bool) error
}

type ByID interface {
	Updater

	// Get get one item by id.
	Get(bean interface{}) error

	// Omit only not use the parameters as select or update columns
	Omit(columns ...string) ByID

	// Cols only use the parameters as update columns
	Columns(columns ...string) ByID
}

// RawResult is an interface that defines methods useful for working with result
// sets.
type RawResult struct {
	RawQueryResult
	instance func() interface{}
}

// Count returns the number of items that match the set conditions. `Offset()`
// and `Limit()` are not honoured by `Count()`
func (result *RawResult) Count() (int64, error) {
	return result.session.Count(result.instance())
}

// ForEach record by record handle records from table, condiBeans's non-empty fields
// are conditions. beans could be []Struct, []*Struct, map[int64]Struct
// map[int64]*Struct
func (result *RawResult) ForEach(cb func(i int, read func(bean interface{}) error) error) error {
	return result.session.Iterate(result.instance(), func(i int, instance interface{}) error {
		return cb(i, func(bean interface{}) error {
			return copyStruct(bean, instance)
		})
	})
}

// RawQueryResult is an interface that defines methods useful for working with result
// sets.
type RawQueryResult struct {
	session *xorm.Session
}

// One fetches the first result within the result set and dumps it into the
// given pointer to struct or pointer to map. The result set is automatically
// closed after picking the element, so there is no need to call Close()
// after using One().
func (result *RawQueryResult) One(ptrToStruct interface{}) error {
	found, err := result.session.Get(ptrToStruct)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// All fetches all results within the result set and dumps them into the
// given pointer to slice of maps or structs.  The result set is
// automatically closed, so there is no need to call Close() after
// using All().
func (result *RawQueryResult) All(beans interface{}) error {
	return result.session.Find(beans)
}

func copyStruct(fromValue, toValue interface{}) error {
	var (
		from = reflect.ValueOf(fromValue)
		to   = reflect.ValueOf(toValue)
	)

	to.Elem().Set(from.Elem())
	return nil
}
