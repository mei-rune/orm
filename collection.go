package orm

import (
	"errors"
	"strings"

	"github.com/go-xorm/xorm"
)

var ErrNotFound = errors.New("record isn't found")
var ErrInsertFail = errors.New("create record fail")

// Collection is an interface that defines methods useful for handling tables.
type Collection struct {
	Engine    *xorm.Engine
	instance  func() interface{}
	tableName string
}

func New(instance func() interface{}) func(engine *xorm.Engine) *Collection {
	return func(engine *xorm.Engine) *Collection {
		tableName := engine.Table(instance()).Statement.TableName()
		return &Collection{Engine: engine, instance: instance, tableName: tableName}
	}
}

// Insert inserts a new item into the collection, it accepts one argument
// that can be either a map or a struct. If the call suceeds, it returns the
// ID of the newly added element as an `interface{}` (the underlying type of
// this ID is unknown and depends on the database adapter). The ID returned
// by Insert() could be passed directly to Find() to retrieve the newly added
// element.
func (collection *Collection) Insert(bean interface{}) (interface{}, error) {
	rowsAffected, err := collection.Engine.Table(collection.tableName).InsertOne(bean)
	if err != nil {
		return nil, err
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

// Update takes a pointer to map or struct and tries to update the
// given item on the collection based on the item's primary keys. Once the
// element is updated, UpdateReturning will query the element that was just
// updated. If the database does not support transactions this method returns
// db.ErrUnsupported
func (collection *Collection) Update(bean interface{}) error {
	_, err := collection.Engine.Table(collection.tableName).Update(bean)
	return err
}

// Exists returns true if the collection exists, false otherwise.
func (collection *Collection) Exists() (bool, error) {
	return collection.Engine.IsTableExist(collection.tableName)
}

// Find defines a new result set with elements from the collection.
func (collection *Collection) Where() *Result {
	return &Result{&QueryResult{collection: collection,
		session:  collection.Engine.Table(collection.tableName),
		instance: collection.instance}}
}

// Name returns the name of the collection.
func (collection *Collection) Name() string {
	return collection.tableName
}

// Id provides converting id as a query condition
func (collection *Collection) Id(id interface{}) *IdResult {
	return &IdResult{collection: collection,
		session:  collection.Engine.Table(collection.tableName),
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
func (result *Result) Where(querystring string, args ...interface{}) *Result {
	result.session = result.session.And(querystring, args)
	return result
}

// Or adds more filtering conditions on top of the existing constraints.
func (result *Result) Or(querystring string, args ...interface{}) *Result {
	result.session = result.session.Or(querystring, args)
	return result
}

// And adds more filtering conditions on top of the existing constraints.
func (result *Result) And(querystring string, args ...interface{}) *Result {
	result.session = result.session.And(querystring, args)
	return result
}

// Delete deletes all items within the result set. `Offset()` and `Limit()` are
// not honoured by `Delete()`.
func (result *Result) Delete() (int64, error) {
	return result.session.Delete(result.instance())
}

// Update modifies all items within the result set. `Offset()` and `Limit()`
// are not honoured by `Update()`.
func (result *Result) Update() (int64, error) {
	return result.session.Update(result.instance())
}

// Count returns the number of items that match the set conditions. `Offset()`
// and `Limit()` are not honoured by `Count()`
func (result *Result) Count() (int64, error) {
	return result.session.Count(result.instance())
}

// QueryResult is an interface that defines methods useful for working with result
// sets.
type QueryResult struct {
	collection *Collection
	session    *xorm.Session
	instance   func() interface{}
}

// Limit defines the maximum number of results in this set. It only has
// effect on `One()`, `All()` and `Next()`.
func (result *QueryResult) Limit(limit int) *QueryResult {
	result.session = result.session.Limit(limit)
	return result
}

// Offset ignores the first *n* results. It only has effect on `One()`, `All()`
// and `Next()`.
func (result *QueryResult) Offset(offset int) *QueryResult {
	result.session.Statement.Start = offset
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
func (result *QueryResult) All(beans interface{}) error {
	return result.session.Find(beans)
}

// IdResult is an interface that defines methods useful for working with result
// sets.
type IdResult struct {
	collection *Collection
	session    *xorm.Session
	instance   func() interface{}
	id         interface{}
}

// Get get one item by id.
func (result *IdResult) Get(bean interface{}) error {
	found, err := result.session.Id(result.id).Get(bean)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

// Delete deletes one item by id.
func (result *IdResult) Delete() error {
	rowsAffected, err := result.session.Id(result.id).Delete(result.instance())
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// Update modifies one item by id.
func (result *IdResult) Update(bean interface{}) error {
	rowsAffected, err := result.session.Id(result.id).Update(bean)
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}
