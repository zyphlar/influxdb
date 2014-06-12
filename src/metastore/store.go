package metastore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"protocol"
)

type Store struct {
	fieldsLock sync.RWMutex

	// Map of databases > series > fields
	StringsToIds map[string]map[string]map[string]uint64
	// Map of ids to the field names. Don't need to know which database or series
	// they track to since we have that information elsewhere
	LastIdUsed       uint64
	clusterConsensus ClusterConsensus
}

type Field struct {
	Id   uint64
	Name string
}

func (f *Field) IdAsBytes() []byte {
	idBytes := make([]byte, 8, 8)
	binary.PutUvarint(idBytes, f.Id)
	return idBytes
}

type ClusterConsensus interface {
	// Will return a collection of series objects that have their name, fields, and fieldIds set.
	// This can be used to set the field ids and fill the cache.
	GetOrSetFieldIdsForSeries(database string, series []*protocol.Series) ([]*protocol.Series, error)
}

func NewStore() *Store {
	return &Store{
		StringsToIds: make(map[string]map[string]map[string]uint64),
	}
}

func NewStoreFromJson(data []byte) (*Store, error) {
	store := &Store{}
	err := json.Unmarshal(data, store)
	return store, err
}

func (self *Store) ToJson() ([]byte, error) {
	return json.Marshal(self)
}

func (self *Store) SetClusterConsensus(c ClusterConsensus) {
	self.clusterConsensus = c
}

func (self *Store) ReplaceFieldNamesWithFieldIds(database string, series []*protocol.Series) error {
	allSetFromCache := self.setFieldIdsFromCache(database, series)
	if allSetFromCache {
		return nil
	}

	// one or more of the fields weren't in the cache, so just get the whole list from Raft
	seriesWithOnlyFields := make([]*protocol.Series, len(series), len(series))
	for i, s := range series {
		seriesWithOnlyFields[i] = &protocol.Series{Name: s.Name, Fields: s.Fields}
	}
	seriesWithFieldIds, err := self.clusterConsensus.GetOrSetFieldIdsForSeries(database, seriesWithOnlyFields)
	if err != nil {
		return err
	}
	fmt.Println("******", seriesWithFieldIds[0:5])
	self.fillCache(database, seriesWithFieldIds)
	fmt.Println("cacheFilled", seriesWithFieldIds[0:5])
	for i, s := range series {
		s.Fields = nil
		s.FieldIds = seriesWithFieldIds[i].FieldIds
	}
	return nil
}

func (self *Store) GetOrSetFieldIds(database string, series []*protocol.Series) error {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	databaseSeries := self.StringsToIds[database]
	if databaseSeries == nil {
		databaseSeries = make(map[string]map[string]uint64)
		self.StringsToIds[database] = databaseSeries
	}
	for _, s := range series {
		fieldIds := make([]uint64, len(s.Fields), len(s.Fields))
		seriesFieldMap := databaseSeries[*s.Name]
		if seriesFieldMap == nil {
			seriesFieldMap = make(map[string]uint64)
			databaseSeries[*s.Name] = seriesFieldMap
		}
		for i, fieldName := range s.Fields {
			fieldId, ok := seriesFieldMap[fieldName]
			if !ok {
				self.LastIdUsed += 1
				seriesFieldMap[fieldName] = self.LastIdUsed
				fieldId = self.LastIdUsed
			}
			fieldIds[i] = fieldId
		}
		s.FieldIds = fieldIds
	}
	return nil
}

func (self *Store) GetSeriesForDatabaseAndRegex(database string, regex *regexp.Regexp) []string {
	return nil
}

func (self *Store) GetSeriesForDatabase(database string) []string {
	return nil
}

func (self *Store) GetFieldsForSeries(database, series string) []*Field {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		return nil
	}
	fieldMap := databaseSeries[series]
	fields := make([]*Field, 0, len(fieldMap))
	for name, id := range fieldMap {
		fields = append(fields, &Field{Name: name, Id: id})
	}
	return fields
}

func (self *Store) DropSeries(database, series string) {

}

func (self *Store) DropDatabase(database string) {

}

func (self *Store) fillCache(database string, series []*protocol.Series) {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		databaseSeries = make(map[string]map[string]uint64)
		self.StringsToIds[database] = databaseSeries
	}
	for _, s := range series {
		seriesFields, ok := databaseSeries[*s.Name]
		if !ok {
			seriesFields = make(map[string]uint64)
			databaseSeries[*s.Name] = seriesFields
		}
		for i, f := range s.Fields {
			seriesFields[f] = s.FieldIds[i]
		}
	}
}

func (self *Store) setFieldIdsFromCache(database string, series []*protocol.Series) bool {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	databaseSeries := self.StringsToIds[database]
	for _, s := range series {
		seriesFields, ok := databaseSeries[*s.Name]
		if !ok {
			return false
		}
		fieldIds := make([]uint64, len(s.Fields), len(s.Fields))
		for i, f := range s.Fields {
			fieldId, ok := seriesFields[f]
			if !ok {
				return false
			}
			fieldIds[i] = fieldId
		}
		s.FieldIds = fieldIds
	}
	return true
}

// Returns an array of field ids that are in the same index as their associated fieldNames.
// If there are fields that don't have ids, they're returned in the second argument.
// func (self *Store) GetFieldIds(database, series string, fieldNames []string) ([]uint64, []string) {
// 	self.fieldsLock.RLock()
// 	defer self.fieldsLock.Unlock()
// 	seriesMap := self.StringsToIds[database]
// 	if seriesMap == nil {
// 		return nil, fieldNames
// 	}
// 	fieldMap := seriesMap[series]
// 	if fieldMap == nil {
// 		return nil, fieldNames
// 	}
// 	fieldIds := make([]uint64, 0)

// }

// // this only gets called from raft and expects an array of unique ids that can
// // be returned
// func (self *Store) AddFields(database, series string, fieldNames []string) ([]uint64, error) {
// 	self.fieldsLock.Lock()
// 	defer self.fieldsLock.Unlock()
// 	seriesMap := self.StringsToIds[database]
// 	if seriesMap == nil {
// 		seriesMap = make(map[string]map[string]uint64)
// 		self.StringsToIds[database] = seriesMap
// 	}
// 	fieldMap := seriesMap[series]
// 	if fieldMap == nil {
// 		fieldMap = make(map[string]uint64)
// 		seriesMap[series] = fieldMap
// 	}
// 	fieldIds := make([]uint64, len(fieldNames), len(fieldNames))
// 	for i, fieldName := range fieldNames {
// 		self.LastIdUsed += 1
// 		fieldMap[fieldName] = self.LastIdUsed
// 		fieldIds[i] = self.LastIdUsed
// 	}
// 	return fieldIds, nil
// }
