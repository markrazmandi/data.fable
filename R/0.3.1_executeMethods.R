# extract data------------------------------------------------------------------
extractData <- function(p,data,d_spec) {

  d_spec <- d_spec[data_hash==p$data_hash]$data_spec[[1]]

  data <- merge(
    x=d_spec,
    y=data,
    by=names(d_spec)
  )

  data <- data[,.SD,.SDcols=-names(d_spec)]

  return(data)

}

# extract model specification---------------------------------------------------
extractModel <- function(p,m_spec) {

  m_spec <- m_spec[m_hash==p$m_hash]$m_spec[[1]]

  return(m_spec)

}

# execute forecast iteration----------------------------------------------------
executeForecast <- function(data,p,d_spec,m_spec,
                            incl_fit=getOption('incl_fit',default=FALSE)) {

  # extract data and model specification----------------------------------------
  data <- extractData(
    p=p,
    data=data,
    d_spec=d_spec
  )

  m_spec <- extractModel(
    p=p,
    m_spec=m_spec
  )

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
  if (incl_fit) {

    p$m_fit <- list(l$m_fit)

  }

  p[,`:=` (
    m_pred=.(l$m_pred)
  )]

  return(p)

}

# execute parallel forecast-----------------------------------------------------
parallelForecast <- function(dt,f_spec,d_spec,m_spec,src_dir,src_pattern,
                             incl_fit=getOption('incl_fit',default=FALSE),
                             n=detectCores(),timeout=86400*30,chunk.size=NULL) {

  clust <- makeCluster(n,timeout=timeout)

  clusterExport(clust,c('dt','f_spec','d_spec','m_spec','src_dir','src_pattern','incl_fit'),environment())

  clusterEvalQ(clust,{

    library(data.table)
    library(magrittr)
    library(prophet)
    library(purrr)
    library(zoo)

    lapply(list.files(src_dir,src_pattern,full.names=TRUE),source)

    options(incl_fit=incl_fit)

  })

  fdb <- parLapplyLB(clust,seq_len(nrow(f_spec)),function(j) {

    # execute forecast iteration------------------------------------------------
    p <- executeForecast(
      data=dt,
      p=f_spec[j],
      d_spec=d_spec,
      m_spec=m_spec,
      incl_fit=getOption('incl_fit',default=FALSE)
      )

    return(p)

  },chunk.size=chunk.size) %>%
    rbindlist(fill=TRUE)

  # kill cluster----------------------------------------------------------------
  stopCluster(clust)

  return(fdb)

}
