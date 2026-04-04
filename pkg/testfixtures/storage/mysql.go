package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/testutils"
)

const (
	mysqlImage           = "mysql:8"
	mysqlContainerPrefix = "openfga-test-mysql-"
	mysqlPort            = nat.Port("3306/tcp")

	mysqlDBPrefix   = "openfga-test-db-"
	mysqlTemplateDB = mysqlDBPrefix + "template"
	mysqlUsername   = "root"
	mysqlPassword   = "secret"
)

var _ DatastoreTestContainer = (*mysqlTestContainer)(nil)

type mysqlTestContainer struct {
	host string
	port string

	database string
	username string
	password string

	version int64
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mysqlTestContainer) GetConnectionURI(includeCredentials bool) string {
	var username, password string
	if includeCredentials {
		username = m.username
		password = m.password
	}

	return mysqlConnectionURI(m.host, m.port, m.database, username, password)
}

func (m *mysqlTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

func (m *mysqlTestContainer) GetUsername() string {
	return m.username
}

func (m *mysqlTestContainer) GetPassword() string {
	return m.password
}

func (m *mysqlTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *mysqlTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

func RunMysqlTestContainer(t testing.TB) DatastoreTestContainer {
	docker, err := testutils.NewDockerClient()
	require.NoError(t, err)

	goose.SetLogger(goose.NopLogger())
	goose.SetBaseFS(assets.EmbedMigrations)

	t.Cleanup(func() {
		docker.Close()
	})

	dockerCont, found, err := docker.FindRunningContainer(t.Context(), "^"+mysqlContainerPrefix, mysqlImage)
	require.NoError(t, err, "find running mysql container")

	if !found {
		dockerCont = bootstrapMysqlContainer(t, docker)
	}

	port, err := docker.GetHostPort(dockerCont, mysqlPort)
	require.NoError(t, err)

	testCont := &mysqlTestContainer{
		host:     "localhost",
		port:     port,
		database: mysqlDBPrefix + ulid.Make().String(),
		username: mysqlUsername,
		password: mysqlPassword,
	}

	tplURI := mysqlConnectionURI(testCont.host, testCont.port, mysqlTemplateDB, testCont.username, testCont.password)
	require.NoError(t, waitForDatabase("mysql", tplURI))

	createExec := container.ExecOptions{
		Cmd: []string{"mysql", "-u", testCont.username, "-e", fmt.Sprintf("CREATE DATABASE `%s`;", testCont.database)},
		Env: []string{"MYSQL_PWD=" + testCont.password},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), dockerCont.ID, createExec))

	copyShell := fmt.Sprintf(
		"mysqldump -u%[1]s %[2]s | mysql -u%[1]s %[3]s",
		testCont.username, mysqlTemplateDB, testCont.database,
	)
	copyExec := container.ExecOptions{
		Cmd: []string{"sh", "-c", copyShell},
		Env: []string{"MYSQL_PWD=" + testCont.password},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), dockerCont.ID, copyExec))
	require.NoError(t, waitForDatabase("mysql", testCont.GetConnectionURI(true)))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", testCont.database)
		dropExec := container.ExecOptions{
			Cmd: []string{"mysql", "-u", testCont.username, "-e", dropQuery},
			Env: []string{"MYSQL_PWD=" + testCont.password},
		}
		if err := docker.ExecCommand(ctx, dockerCont.ID, dropExec); err != nil {
			t.Errorf("drop test database in the mysql container: %v", err)
		}
	})

	db, err := goose.OpenDBWithDriver("mysql", testCont.GetConnectionURI(true))
	require.NoError(t, err)
	defer db.Close()

	migrations, err := goose.CollectMigrations(assets.MySQLMigrationDir, 0, goose.MaxVersion)
	require.NoError(t, err)
	require.NotEmpty(t, migrations)
	expVersion := migrations[len(migrations)-1].Version

	require.Eventually(t, func() bool {
		version, err := goose.GetDBVersion(db)
		if err != nil {
			return false
		}
		testCont.version = version
		return version == expVersion
	}, 1*time.Minute, 1*time.Second, "wait for mysql database to be ready")

	return testCont
}

func bootstrapMysqlContainer(t testing.TB, docker *testutils.DockerClient) *container.InspectResponse {
	t.Logf("No running container found for %s, creating a new one", mysqlImage)

	require.NoError(t, docker.PullImage(t.Context(), mysqlImage), "pull mysql image")

	contCfg := &container.Config{
		Env: []string{
			"MYSQL_DATABASE=" + mysqlTemplateDB,
			"MYSQL_ROOT_PASSWORD=" + mysqlPassword,
		},
		ExposedPorts: nat.PortSet{
			mysqlPort: {},
		},
		Image: mysqlImage,
	}

	hostCfg := &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": ""},
	}

	containerName := mysqlContainerPrefix + ulid.Make().String()
	cont, err := docker.RunContainer(t.Context(), contCfg, hostCfg, containerName)
	require.NoError(t, err, "run mysql container")

	port, err := docker.GetHostPort(cont, mysqlPort)
	require.NoError(t, err)

	dbURI := mysqlConnectionURI("localhost", port, mysqlTemplateDB, mysqlUsername, mysqlPassword)
	require.NoError(t, waitForDatabase("mysql", dbURI))

	db, err := goose.OpenDBWithDriver("mysql", dbURI)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, goose.Up(db, assets.MySQLMigrationDir))

	return cont
}

func mysqlConnectionURI(host, port, database, username, password string) string {
	creds := ""
	if username != "" && password != "" {
		creds = fmt.Sprintf("%s:%s@", username, password)
	}

	return fmt.Sprintf("%stcp(%s:%s)/%s?parseTime=true", creds, host, port, database)
}
