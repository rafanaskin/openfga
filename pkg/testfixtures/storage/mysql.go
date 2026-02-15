package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	mySQLContainerName = "open-fga-mysql"
	mySQLImage         = "mysql:8"
)

var (
	mySQLOnce sync.Once
)

type mySQLTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMySQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for MySQL.
func NewMySQLTestContainer() *mySQLTestContainer {
	return &mySQLTestContainer{}
}

func (m *mySQLTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

// RunMySQLTestContainer runs a MySQL container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// MySQL datastore engine.
func (m *mySQLTestContainer) RunMySQLTestContainer(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	ctx := context.Background()

	images, err := dockerClient.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("reference", mySQLImage),
		),
	})
	require.NoError(t, err)

	if len(images) == 0 {
		t.Logf("Pulling image %s", mySQLImage)
		reader, err := dockerClient.ImagePull(ctx, mySQLImage, image.PullOptions{})
		require.NoError(t, err)

		_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
		require.NoError(t, err)
	}

	name := mySQLContainerName + ulid.Make().String()
	cont, found, err := getRunningContainer(ctx, dockerClient, mySQLContainerName, mySQLImage)
	require.NoError(t, err, "failed to find running mysql container")

	if !found {
		mySQLOnce.Do(func() {
			cont, err = runMySQLContainer(ctx, dockerClient, name)
			require.NoError(t, err, "failed to run mysql container")
		})
	}

	p, ok := cont.NetworkSettings.Ports["3306/tcp"]
	if !ok || len(p) == 0 {
		require.Fail(t, "failed to get host port mapping from mysql container")
	}

	mySQLTestContainer := &mySQLTestContainer{
		addr:     "localhost:" + p[0].HostPort,
		username: "root",
		password: "secret",
	}

	uri := mySQLTestContainer.username + ":" + mySQLTestContainer.password + "@tcp(" + mySQLTestContainer.addr + ")/defaultdb?parseTime=true"

	err = mysql.SetLogger(log.New(io.Discard, "", 0))
	require.NoError(t, err)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("mysql", uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 2 * time.Minute
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	require.NoError(t, err, "failed to connect to mysql container")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MySQLMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mySQLTestContainer.version = version

	return mySQLTestContainer
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mySQLTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = m.username + ":" + m.password + "@"
	}

	return creds + "tcp(" + m.addr + ")/defaultdb?parseTime=true"
}

func (m *mySQLTestContainer) GetUsername() string {
	return m.username
}

func (m *mySQLTestContainer) GetPassword() string {
	return m.password
}

func (m *mySQLTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *mySQLTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

func runMySQLContainer(ctx context.Context, cli *client.Client, containerName string) (*container.InspectResponse, error) {
	containerCfg := container.Config{
		Env: []string{
			"MYSQL_DATABASE=" + databaseName,
			"MYSQL_ROOT_PASSWORD=secret",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("3306/tcp"): {},
		},
		Image: mySQLImage,
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": ""},
	}

	cont, err := cli.ContainerCreate(ctx, &containerCfg, &hostCfg, nil, nil, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql docker container: %w", err)
	}

	err = cli.ContainerStart(ctx, cont.ID, container.StartOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to start mysql container: %w", err)
	}

	inspect, err := cli.ContainerInspect(context.Background(), cont.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect %s container: %w", containerName, err)
	}

	return &inspect, nil
}
