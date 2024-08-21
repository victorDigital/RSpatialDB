library(dotenv)
library(RPostgreSQL)


data_folder <- "exampleData"



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
    q("no")
  }
)

# DEV: drop all tables to start fresh
#for (table in dbListTables(connec)) {
#  dbSendQuery(connec, paste0("DROP TABLE ", table))
#}

tables <- dbListTables(connec)

if (!"data" %in% tables) {
  print("Creating table data because it does not exist.")
  dbSendQuery(
    connec,
    "CREATE TABLE data (
      sat VARCHAR(10),
      time TEXT,
      decyear TEXT,
      lat TEXT,
      lon TEXT,
      himth TEXT,
      himth05 TEXT,
      geoid TEXT,
      distnode TEXT,
      width TEXT,
      reachid TEXT,
      nodeid TEXT,
      wse TEXT,
      ocval TEXT,
      height TEXT,
      heighte TEXT,
      waterid TEXT,
      hocean TEXT,
      hocog TEXT,
      hice1 TEXT,
      hice2 TEXT,
      PRIMARY KEY (sat, time, lat, lon)
    );"
    # text is used for all columns because the data is not consistent
  )
}


folders <- list.dirs(path = paste0("./", data_folder), full.names = FALSE, recursive = FALSE)

for (folder in folders) {
  files <- list.files(path = paste0("./", data_folder, "/", folder), full.names = FALSE, recursive = FALSE, pattern = ".dat")
  for (file in files) {
    if (grepl(".Identifier", file)) {
      next
    }
    print(file)
    data <- read.csv(sep = " ", paste0("./", data_folder, "/", folder, "/", file))
    data$sat <- folder

    names(data) <- tolower(names(data))

    dbWriteTable(connec, "data", data, append = TRUE, row.names = FALSE)
  }
}

#dump the entire db to a csv file
#query <- "SELECT * FROM data"
#data <- dbGetQuery(connec, query)
#write.csv(data, "data.csv", row.names = FALSE)



# Close the connection on exit
on.exit(dbDisconnect(connec), add = TRUE)