package metastore

import (
	"encoding/json"
	"sync"
)

type Store struct {
	fieldsLock sync.RWMutex

	// Map of databases > series > fields > id
	StringsToIds map[string]map[string]map[string]uint64
	// Map of ids to the field names. Don't need to know which database or series
	// they track to since we have that information elsewhere
	IdsToFieldNames map[uint64]string
	LastIdUsed      uint64
}

func NewStore() *Store {
	return &Store{
		StringsToIds:    make(map[string]map[string]map[string]uint64),
		IdsToFieldNames: make(map[uint64]string)}
}

func NewStoreFromJson(data []byte) (*Store, error) {
	store := &Store{}
	err := json.Unmarshal(data, store)
	return store, err
}

func (self *Store) ToJson() ([]byte, error) {
	return json.Marshal(self)
}

// Returns an array of field ids that are in the same index as their associated fieldNames.
// If there are fields that don't have ids, they're returned in the second argument.
func (self *Store) GetFieldIds(database, series string, fieldNames []string) ([]uint64, []string) {
	self.fieldsLock.RLock()
	defer self.fieldsLock.Unlock()
	seriesMap := self.StringsToIds[database]
	if seriesMap == nil {
		return nil, fieldNames
	}
	fieldMap := seriesMap[series]
	if fieldMap == nil {
		return nil, fieldNames
	}
	fieldIds := make([]uint64, 0)

}

// this only gets called from raft and expects an array of unique ids that can
// be returned
func (self *Store) AddFields(database, series string, fieldNames []string) ([]uint64, error) {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	seriesMap := self.StringsToIds[database]
	if seriesMap == nil {
		seriesMap = make(map[string]map[string]uint64)
		self.StringsToIds[database] = seriesMap
	}
	fieldMap := seriesMap[series]
	if fieldMap == nil {
		fieldMap = make(map[string]uint64)
		seriesMap[series] = fieldMap
	}
	fieldIds := make([]uint64, len(fieldNames), len(fieldNames))
	for i, fieldName := range fieldNames {
		self.LastIdUsed += 1
		fieldMap[fieldName] = self.LastIdUsed
		fieldIds[i] = self.LastIdUsed
	}
	return fieldIds, nil
}
