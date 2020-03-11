# pairlist to extract data based on source--------------------------------------
dataInclusion <- function(dt,key,incl_data) {

  if (incl_data) {

    dots <- list(dt=NULL,key=NULL)

  } else {

    dots <- list(dt=dt,key=key)

  }

  return(dots)

}

# extract data------------------------------------------------------------------
extractData <- function(p,dots) {

  if (is.null(dots$dt)) {

    data <- copy(p$data[[1]])

  } else {

    data <- merge(
      x=p[,.SD,.SDcols=dots$key],
      y=dots$dt,
      by=dots$key
    )

  }

  return(data)

}

# extract model specification---------------------------------------------------
extractModel <- function(p) {

  p$m_spec[[1]]

}

# forecast model switch---------------------------------------------------------
forecastModels <- function(m,data,m_spec,train_start,test_start,test_end) {

  switch(
    EXPR=m,
    'prophet'=prophetForecast(
      data=data,
      m_spec=m_spec,
      train_start=train_start,
      test_start=test_start,
      test_end=test_end
    ),
    'ma'=rollingMean(
      data=data,
      m_spec=m_spec,
      train_start=train_start,
      test_start=test_start,
      test_end=test_end
    )
  )

}

# execute forecast iteration----------------------------------------------------
executeForecast <- function(p,dots) {

  # extract data and model specification----------------------------------------
  data <- extractData(p=p,dots=dots)
  m_spec <- extractModel(p=p)

  # forecast model switch-------------------------------------------------------
  l <- forecastModels(
    m=p$m,
    data=data,
    m_spec=m_spec,
    train_start=p$train_start,
    test_start=p$test_start,
    test_end=p$test_end
  )

  # assign model fit and predictions--------------------------------------------
  p[,`:=` (
    m_fit=.(l$m_fit),
    m_pred=.(l$m_pred)
  )]

  return(p)

}

# execute sequential forecast---------------------------------------------------
sequentialForecast <- function(gdb,dt,key,incl_data) {

  dots <- dataInclusion(dt,key,incl_data)

  fdb <- lapply(seq_len(nrow(gdb)),function(j) {

    # subset parameters---------------------------------------------------------
    p <- gdb[j]

    # execute forecast iteration------------------------------------------------
    p <- executeForecast(p=p,dots=dots)

    return(p)

  }) %>% rbindlist(fill=TRUE)

  return(fdb)

}

# create cluster and initialize node environments-------------------------------
setupCluster <- function(gdb,dt,key,incl_data,src_dir,
                         n=detectCores(),timeout=86400*30) {

  dots <- dataInclusion(dt,key,incl_data)

  clust <- makeCluster(n,timeout=timeout)

  clusterExport(clust,c('src_dir','gdb','dots'),environment())

  clusterEvalQ(clust,{

    library(data.table)
    library(magrittr)
    library(prophet)
    library(zoo)

    lapply(list.files(src_dir,'[0-9].[0-9].[1-9]',full.names=TRUE),source)

  })

  return(clust)

}

# execute parallel forecast-----------------------------------------------------
parallelForecast <- function(clust,gdb,chunk.size=NULL) {

  fdb <- parLapplyLB(clust,seq_len(nrow(gdb)),function(j) {

    # subset parameters---------------------------------------------------------
    p <- gdb[j]

    # execute forecast iteration------------------------------------------------
    p <- executeForecast(p=p,dots=dots)

    return(p)

  },chunk.size=chunk.size) %>%
    rbindlist(fill=TRUE)

  return(fdb)

}
