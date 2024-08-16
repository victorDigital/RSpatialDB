library(dotenv)
library(RPostgreSQL)

load_dot_env(file = ".env")

dsn_database <- Sys.getenv("DB_NAME")
dsn_hostname <- Sys.getenv("DB_HOST")
dsn_port <- Sys.getenv("DB_PORT")
dsn_uid <- Sys.getenv("DB_UID")
dsn_pwd <- Sys.getenv("DB_PASS")

# Create a connection to the database
tryCatch(
  {
    drv <- dbDriver("PostgreSQL")
    print("Connecting to Database…")
    connec <- dbConnect(
      drv,
      dbname = dsn_database,
      host = dsn_hostname,
      port = dsn_port,
      user = dsn_uid,
      password = dsn_pwd
    )
    print("Database Connected!")
  },
  error = function(cond) {
    print("Unable to connect to Database.")
    print(cond)
    return(NULL)
  }
)

# TODO: DO DB STUFF HERE

# Close the connection on exit
on.exit(dbDisconnect(connec), add = TRUE)