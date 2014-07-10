package cluster

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"common"
)

type ShardSpace struct {
	// required, must be unique within in the cluster
	Name string
	// optional, if they don't set this shard space will get evaluated for every database
	Database string
	// this is optional, if they don't set it, we'll set to /.*/
	Regex         string
	CompiledRegex *regexp.Regexp `json:"-"`
	// this is optional, if they don't set it, it will default to the storage.dir in the config
	RetentionPolicy   string
	ShardDuration     string
	ReplicationFactor uint32
	Split             uint32
	Shards            []*ShardData `json:"-"`
}

const (
	SEVEN_DAYS                        = time.Hour * 24 * 7
	DEFAULT_SPLIT                     = 1
	DEFAULT_REPLICATION_FACTOR        = 1
	DEFAULT_SHARD_DURATION            = SEVEN_DAYS
	DEFAULT_RETENTION_POLICY_DURATION = 0
)

func NewShardSpace(name string) *ShardSpace {
	s := &ShardSpace{
		Name:   name,
		Shards: make([]*ShardData, 0),
	}
	s.SetDefaults()
	return s
}

func (s *ShardSpace) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("Shard space must have a name")
	}
	if s.Name == "default" {
		return fmt.Errorf("'default' is a reserved shard space name")
	}
	if s.Regex != "" {
		reg := s.Regex
		if strings.HasPrefix(reg, "/") {
			if strings.HasSuffix(reg, "/i") {
				reg = fmt.Sprintf("(?i)%s", reg[1:len(reg)-2])
			} else {
				reg = reg[1 : len(reg)-1]
			}
		}
		r, err := regexp.Compile(reg)
		if err != nil {
			return fmt.Errorf("Error parsing regex: %s", err)
		}
		s.CompiledRegex = r
	}
	if s.Split == 0 {
		s.Split = DEFAULT_SPLIT
	}
	if s.ReplicationFactor == 0 {
		s.ReplicationFactor = DEFAULT_REPLICATION_FACTOR
	}
	if s.ShardDuration != "" {
		if _, err := common.ParseTimeDuration(s.ShardDuration); err != nil {
			return err
		}
	}
	if s.RetentionPolicy != "" {
		if _, err := common.ParseTimeDuration(s.RetentionPolicy); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardSpace) SetDefaults() {
	r, _ := regexp.Compile(".*")
	s.CompiledRegex = r
	s.Split = DEFAULT_SPLIT
	s.ReplicationFactor = DEFAULT_REPLICATION_FACTOR
}

func (s *ShardSpace) MatchesSeries(name string) bool {
	if s.CompiledRegex == nil {
		s.SetDefaults()
	}
	return s.CompiledRegex.MatchString(name)
}

func (s *ShardSpace) SecondsOfDuration() float64 {
	return s.ParsedShardDuration().Seconds()
}

func (s *ShardSpace) ParsedRetentionPeriod() time.Duration {
	if s.RetentionPolicy == "" {
		return DEFAULT_RETENTION_POLICY_DURATION
	}
	d, _ := common.ParseTimeDuration(s.RetentionPolicy)
	return time.Duration(d)
}

func (s *ShardSpace) ParsedShardDuration() time.Duration {
	if s.ShardDuration != "" {
		d, _ := common.ParseTimeDuration(s.ShardDuration)
		return time.Duration(d)
	}
	return DEFAULT_SHARD_DURATION
}
