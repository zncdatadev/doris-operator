package doris_client

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	ctrl "sigs.k8s.io/controller-runtime"
)

var clientLogger = ctrl.Log.WithName("doris-client")
var authLogger = ctrl.Log.WithName("doris-auth")

const (
	// defaultQueryPort is the default MySQL query port for Doris FE
	defaultQueryPort = 9030

	// defaultConnectionTimeout is the timeout for establishing a MySQL connection
	defaultConnectionTimeout = 10 * time.Second

	// defaultQueryTimeout is the timeout for executing a query
	defaultQueryTimeout = 30 * time.Second

	// defaultAdminUser is the default admin username when not specified in Secret
	defaultAdminUser = "root"
)

// FrontendInfo represents information about a Doris FE node
type FrontendInfo struct {
	Name        string
	Host        string
	EditLogPort int
	QueryPort   int
	Role        string // FOLLOWER, OBSERVER, MASTER
	IsMaster    bool
	Alive       bool
}

// BackendInfo represents information about a Doris BE node
type BackendInfo struct {
	Name         string
	Host         string
	Port         int
	Alive        bool
	Decommission bool
	TabletNum    int
}

// BrokerInfo represents information about a Doris Broker node
type BrokerInfo struct {
	Name  string
	Host  string
	Port  int
	Alive bool
}

// DorisClient wraps a MySQL connection to Doris FE
type DorisClient struct {
	db *sql.DB
}

// NewDorisClient creates a new DorisClient connecting to the FE service
func NewDorisClient(feHost string, fePort int, user, password string) (*DorisClient, error) {
	if fePort == 0 {
		fePort = defaultQueryPort
	}

	dsn := mysql.NewConfig()
	dsn.User = user
	dsn.Passwd = password
	dsn.Net = "tcp"
	dsn.Addr = fmt.Sprintf("%s:%d", feHost, fePort)
	dsn.Timeout = defaultConnectionTimeout
	dsn.ReadTimeout = defaultQueryTimeout
	dsn.WriteTimeout = defaultQueryTimeout

	db, err := sql.Open("mysql", dsn.FormatDSN())

	if err != nil {
		return nil, fmt.Errorf("failed to open connection to FE %s:%d: %w", feHost, fePort, err)
	}

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultConnectionTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping FE %s:%d: %w", feHost, fePort, err)
	}

	clientLogger.Info("Connected to Doris FE", "host", feHost, "port", fePort)
	return &DorisClient{db: db}, nil
}

// Close closes the MySQL connection
func (c *DorisClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// queryRows executes a query and returns the raw rows
func (c *DorisClient) queryRows(ctx context.Context, query string) (*sql.Rows, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	return c.db.QueryContext(ctx, query)
}

// ShowFrontends returns all FE nodes from the cluster
func (c *DorisClient) ShowFrontends(ctx context.Context) ([]FrontendInfo, error) {
	rows, err := c.queryRows(ctx, "SHOW FRONTENDS")
	if err != nil {
		return nil, fmt.Errorf("failed to show frontends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var frontends []FrontendInfo
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get frontend columns: %w", err)
	}

	// Build column name to index map for reliable parsing
	colIdx := make(map[string]int)
	for i, name := range columns {
		colIdx[strings.ToUpper(name)] = i
	}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("failed to scan frontend row: %w", err)
		}

		fe := FrontendInfo{}
		if idx, ok := colIdx["NAME"]; ok {
			fe.Name = values[idx].String
		}
		if idx, ok := colIdx["HOST"]; ok {
			fe.Host = values[idx].String
		}
		if idx, ok := colIdx["EDITLOGPORT"]; ok && values[idx].Valid {
			_, _ = fmt.Sscanf(values[idx].String, "%d", &fe.EditLogPort)
		}
		if idx, ok := colIdx["QUERYPORT"]; ok && values[idx].Valid {
			_, _ = fmt.Sscanf(values[idx].String, "%d", &fe.QueryPort)
		}
		if idx, ok := colIdx["ROLE"]; ok {
			fe.Role = values[idx].String
		}
		if idx, ok := colIdx["ISMASTER"]; ok {
			fe.IsMaster = strings.EqualFold(values[idx].String, "true")
		}
		if idx, ok := colIdx["ALIVE"]; ok {
			fe.Alive = strings.EqualFold(values[idx].String, "true")
		}

		frontends = append(frontends, fe)
	}

	_ = rows.Close()
	return frontends, nil
}

// GetMasterFe returns the Master FE node info
func (c *DorisClient) GetMasterFe(ctx context.Context) (*FrontendInfo, error) {
	frontends, err := c.ShowFrontends(ctx)
	if err != nil {
		return nil, err
	}

	for _, fe := range frontends {
		if fe.IsMaster && fe.Alive {
			return &fe, nil
		}
	}

	return nil, fmt.Errorf("no alive master FE found")
}

// GetFollowers returns all alive FE follower nodes
func (c *DorisClient) GetFollowers(ctx context.Context) ([]FrontendInfo, error) {
	frontends, err := c.ShowFrontends(ctx)
	if err != nil {
		return nil, err
	}

	var followers []FrontendInfo
	for _, fe := range frontends {
		if fe.Role == "FOLLOWER" && fe.Alive {
			followers = append(followers, fe)
		}
	}
	return followers, nil
}

// ShowBackends returns all BE nodes from the cluster
func (c *DorisClient) ShowBackends(ctx context.Context) ([]BackendInfo, error) {
	rows, err := c.queryRows(ctx, "SHOW BACKENDS")
	if err != nil {
		return nil, fmt.Errorf("failed to show backends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var backends []BackendInfo
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get backend columns: %w", err)
	}

	colIdx := make(map[string]int)
	for i, name := range columns {
		colIdx[strings.ToUpper(name)] = i
	}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("failed to scan backend row: %w", err)
		}

		be := BackendInfo{}
		if idx, ok := colIdx["NAME"]; ok {
			be.Name = values[idx].String
		}
		if idx, ok := colIdx["HOST"]; ok {
			be.Host = values[idx].String
		}
		if idx, ok := colIdx["HEARTBEATPORT"]; ok && values[idx].Valid {
			_, _ = fmt.Sscanf(values[idx].String, "%d", &be.Port)
		}
		if idx, ok := colIdx["ALIVE"]; ok {
			be.Alive = strings.EqualFold(values[idx].String, "true")
		}
		if idx, ok := colIdx["DECOMMISSION"]; ok {
			be.Decommission = strings.EqualFold(values[idx].String, "true")
		}
		if idx, ok := colIdx["TABLETNUM"]; ok && values[idx].Valid {
			_, _ = fmt.Sscanf(values[idx].String, "%d", &be.TabletNum)
		}

		backends = append(backends, be)
	}

	_ = rows.Close()
	return backends, nil
}

// ShowBrokers returns all Broker nodes from the cluster
func (c *DorisClient) ShowBrokers(ctx context.Context) ([]BrokerInfo, error) {
	rows, err := c.queryRows(ctx, "SHOW BROKER")
	if err != nil {
		return nil, fmt.Errorf("failed to show brokers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var brokers []BrokerInfo
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get broker columns: %w", err)
	}

	colIdx := make(map[string]int)
	for i, name := range columns {
		colIdx[strings.ToUpper(name)] = i
	}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("failed to scan broker row: %w", err)
		}

		bi := BrokerInfo{}
		if idx, ok := colIdx["NAME"]; ok {
			bi.Name = values[idx].String
		}
		if idx, ok := colIdx["HOST"]; ok {
			bi.Host = values[idx].String
		}
		if idx, ok := colIdx["PORT"]; ok && values[idx].Valid {
			_, _ = fmt.Sscanf(values[idx].String, "%d", &bi.Port)
		}
		if idx, ok := colIdx["ALIVE"]; ok {
			bi.Alive = strings.EqualFold(values[idx].String, "true")
		}

		brokers = append(brokers, bi)
	}

	_ = rows.Close()
	return brokers, nil
}

// DecommissionBackend safely decommissions a BE node
func (c *DorisClient) DecommissionBackend(ctx context.Context, host string, port int) error {
	query := fmt.Sprintf("ALTER SYSTEM DECOMMISSION BACKEND \"%s:%d\"", host, port)
	return c.exec(ctx, query)
}

// DropBackend forcibly removes a BE node
func (c *DorisClient) DropBackend(ctx context.Context, host string, port int) error {
	query := fmt.Sprintf("ALTER SYSTEM DROP BACKEND \"%s:%d\"", host, port)
	return c.exec(ctx, query)
}

// DropObserver removes an FE observer node
func (c *DorisClient) DropObserver(ctx context.Context, host string, port int) error {
	query := fmt.Sprintf("ALTER SYSTEM DROP OBSERVER \"%s:%d\"", host, port)
	return c.exec(ctx, query)
}

// exec executes a DDL/management statement
func (c *DorisClient) exec(ctx context.Context, query string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()
	_, err := c.db.ExecContext(ctx, query)
	return err
}

// InitializeAdminUser creates an admin user in Doris (if not exists) and grants
// NODE_PRIV and GRANT_PRIV at the global level.
// This should be called with the root user credentials.
func (c *DorisClient) InitializeAdminUser(ctx context.Context, username, password string) error {
	if username == "" {
		return fmt.Errorf("admin username must not be empty")
	}

	// Create user if not exists
	createUserSQL := fmt.Sprintf(
		"CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'",
		escapeSQLString(username), escapeSQLString(password),
	)
	if err := c.exec(ctx, createUserSQL); err != nil {
		return fmt.Errorf("failed to create admin user %s: %w", username, err)
	}
	authLogger.Info("Created admin user", "user", username)

	// Grant NODE_PRIV and GRANT_PRIV at global level (*.*.*)
	grantSQL := fmt.Sprintf(
		"GRANT NODE_PRIV, GRANT_PRIV ON *.*.* TO '%s'@'%%'",
		escapeSQLString(username),
	)
	if err := c.exec(ctx, grantSQL); err != nil {
		return fmt.Errorf("failed to grant privileges to admin user %s: %w", username, err)
	}
	authLogger.Info("Granted NODE_PRIV and GRANT_PRIV to admin user", "user", username)

	return nil
}

// CheckUserExists checks if a Doris user exists by querying the mysql.user table.
func (c *DorisClient) CheckUserExists(ctx context.Context, username string) (bool, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM mysql.user WHERE user_name = '%s'",
		escapeSQLString(username),
	)

	ctx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
	defer cancel()

	var count int
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check user existence for %s: %w", username, err)
	}

	return count > 0, nil
}

// IsDecommissionComplete checks if a BE has finished decommissioning
func IsDecommissionComplete(be BackendInfo) bool {
	return be.Decommission && be.TabletNum == 0
}

// ResolvePodHost resolves the DNS name for a pod
func ResolvePodHost(podName, namespace, clusterDomain string) string {
	if clusterDomain == "" {
		clusterDomain = "cluster.local"
	}
	return fmt.Sprintf("%s.%s.svc.%s", podName, namespace, clusterDomain)
}

// MatchPodToBackend matches a K8s pod name to a Doris BE node by hostname substring match.
// Doris registers nodes using their pod hostname, so string matching is sufficient.
func MatchPodToBackend(podName string, backends []BackendInfo) *BackendInfo {
	for i := range backends {
		be := &backends[i]
		if strings.Contains(be.Host, podName) || be.Host == podName {
			return be
		}
	}
	return nil
}

// MatchPodToFrontend matches a K8s pod name to a Doris FE node by hostname substring match.
// Doris registers nodes using their pod hostname, so string matching is sufficient.
func MatchPodToFrontend(podName string, frontends []FrontendInfo) *FrontendInfo {
	for i := range frontends {
		fe := &frontends[i]
		if strings.Contains(fe.Host, podName) || fe.Host == podName {
			return fe
		}
	}
	return nil
}

// escapeSQLString escapes single quotes and backslashes in SQL string values
// using MySQL double-escape convention (” for ', \\ for \).
func escapeSQLString(s string) string {
	var result strings.Builder
	result.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 0x27: // single quote
			result.WriteByte(0x27)
			result.WriteByte(0x27)
		case 0x5c: // backslash
			result.WriteByte(0x5c)
			result.WriteByte(0x5c)
		default:
			result.WriteByte(s[i])
		}
	}
	return result.String()
}

// GetClusterAuthCredentials extracts admin username and password from a Secret's data map.
// If username key is not present, defaults to "root".
// If password key is not present, returns empty password.
func GetClusterAuthCredentials(secretData map[string][]byte) (username, password string) {
	username = defaultAdminUser
	password = ""

	if secretData == nil {
		return username, password
	}

	if val, ok := secretData["username"]; ok && len(val) > 0 {
		username = string(val)
	}
	if val, ok := secretData["password"]; ok {
		password = string(val)
	}

	return username, password
}
