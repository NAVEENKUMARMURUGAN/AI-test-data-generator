# Pull the PostgreSQL Docker image
docker pull postgres

# Run the PostgreSQL container
docker run --name postgres-sample -e POSTGRES_PASSWORD=postgres -d -p postgres 5432:5432 postgres

# Wait a few seconds for PostgreSQL to start
sleep 5

# Connect to the PostgreSQL shell
docker exec -it postgres-sample psql -U postgres -c "
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (username, email) VALUES
('NAVEENKUMAR MURUGAN', 'mndlsoft@gmail.com'),
('MURUGAN', 'mndlsof1@gmail.com');

COMMIT;
"
