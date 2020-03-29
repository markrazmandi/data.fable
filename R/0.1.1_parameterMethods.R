# default return values if null-------------------------------------------------
'%set%' <- function(x,y) {

  ifelse(is.null(x),y,x)

}

# subset data groups for granularity--------------------------------------------
dataParameterGrid <- function(dt,subset=NULL,
                              group_by=getAttrs(dt,'group_by'),
                              decreasing=getOption('decreasing',default=FALSE)) {

  # sort group by columns for consistent hashing--------------------------------
  group_by <- sort(group_by,decreasing=decreasing)

  # create nested tables by group-----------------------------------------------
  dt <- dt[eval(subset),.(data_spec=.(unique(.SD))),.SDcols=group_by,by=group_by]

  # create hash for referencing-------------------------------------------------
  dt <- dt[,.(
    data_spec,
    data_hash=as.factor(md5(as.character(data_spec))),
    dummy=0L
  )]

  return(dt)

}

# create relative sequence based on start date----------------------------------
shiftDate <- function(start,step,freq,folds) {

  seq.Date(
    from=as.Date(start),
    by=paste(step,freq),
    length.out=folds
  )

}

# add variable periods to date--------------------------------------------------
addPeriods <- function(date,freq,x,incl_bound=TRUE) {

  date <- as.Date(date)

  if (!incl_bound) {

    x <- x-1

  }

  switch(
    EXPR=freq,
    year=date+years(x),
    month=date+months(x),
    week=date+weeks(x),
    day=date+days(x)
  )

}

# create cross validation window grid-------------------------------------------
crossValidationGrid <- function(
  frequency,
  horizon_length,
  week_start=getOption('week_start',1L),
  train_start,
  test_start,
  cv_folds,
  cv_step,
  cv_window
) {

  # roll train start date to ceiling--------------------------------------------
  train_start <- ceiling_date(
    x=as.IDate(train_start),
    unit=frequency,
    week_start=week_start,
    change_on_boundary=FALSE
  )

  # roll test start date to floor-----------------------------------------------
  test_start <- floor_date(
    x=as.IDate(test_start),
    unit=frequency,
    week_start=week_start
  )

  # compute training start folds based on defined window------------------------
  train_start_folds <- switch(
    EXPR=cv_window,
    slide=shiftDate(
      start=train_start,
      step=cv_step,
      freq=frequency,
      folds=cv_folds
    ),
    stretch=train_start
  )

  # compute relative test start folds-------------------------------------------
  test_start_folds <- shiftDate(
    start=test_start,
    step=-cv_step,
    freq=frequency,
    folds=cv_folds
  )

  # add periods to test start folds according to horizon------------------------
  test_end_folds <- addPeriods(
    date=test_start_folds,
    freq=frequency,
    x=horizon_length,
    incl_bound=FALSE
  )

  # construct cross validaiton table--------------------------------------------
  dt <- data.table(
    train_start=train_start_folds,
    test_start=test_start_folds,
    test_end=test_end_folds,
    horizon_length=horizon_length,
    frequency=as.factor(frequency),
    week_start=week_start,
    dummy=0L
  )

  if (frequency!='week') {

    dt[,week_start := NULL]

  }

  return(dt)

}

# sort list elements and discard null elements----------------------------------
compactList <- function(l,decreasing=getOption('decreasing',default=FALSE)) {

  compact(l[sort(names(l),decreasing=decreasing)])

}

# cartesian join tables within the same depth-----------------------------------
reduceGridList <- function(l) {

  r <- Reduce(f=function(...) {

    merge(...,by='dummy',allow.cartesian=TRUE)

  },x=l)

  return(r)

}

# transpose list columns into nested list column--------------------------------
transposeGridList <- function(r,y) {

  r[,(y) := purrr::transpose(.SD),.SDcols=-c('dummy')]

  t <- r[,.SD,.SDcols=c(y,'dummy')]

  return(t)

}

# cartiesian product of vectors and transpose list into table-------------------
expandGridList <- function(x,y) {

  t <- expand.grid(x,stringsAsFactors=FALSE) %>%
    purrr::transpose() %>%
    data.table(dummy=0L)

  t <- setnames(t,'.',y)

  return(t)

}

# recursive cartesian product creation------------------------------------------
imapGridList <- function(l,decreasing=getOption('decreasing',default=FALSE)) {

  # sort root list elements and discard null elements---------------------------
  l <- compactList(l,decreasing)

  imap(l,function(x,y) {

    # test for nested lists-----------------------------------------------------
    if (every(x,is.list)) {

      # recursively call parent function----------------------------------------
      l <- imapGridList(x)

      # cartesian join tables within the same depth-----------------------------
      r <- reduceGridList(l)

      # transpose list columns into nested list column--------------------------
      t <- transposeGridList(r,y)

      return(t)

    } else {

      # sort list elements and discard null elements----------------------------
      x <- compactList(x,decreasing)

      # cartiesian product of vectors and transpose list into table-------------
      t <- expandGridList(x,y)

      return(t)

    }

  })

}

# apply cartesian grid over list and transpose reduction------------------------
modelParameterGrid <- function(l,decreasing=getOption('decreasing',default=FALSE)) {

  # recursive cartesian product creation----------------------------------------
  m_spec <- imapGridList(l) %>%
    imap(~ {

      # rename model specifications column--------------------------------------
      setnames(.x,.y,'m_spec')

      # assign model names hash model specifications----------------------------
      .x[,`:=` (
        m=as.factor(.y),
        m_hash=as.factor(md5(as.character(m_spec)))
      )]

    }) %>% rbindlist(fill=TRUE)

  return(m_spec)

}

# bind parameter specification tables-------------------------------------------
bindParameterSpecs <- function(l,by) {

  rbindlist(l,use.names=TRUE) %>%
    unique(by=by) %>%
    .[,.SD,.SDcols=-c('dummy')]

}

# timeseries forecast parameter grid--------------------------------------------
forecastParameterGrid <- function(l) {

  # extract data specifications-------------------------------------------------
  dt_spec_l <- map(.x=l,.f=~ {

    # subset data groups for granularity----------------------------------------
    dataParameterGrid(
      dt=pluck(.x,'data'),
      subset=pluck(.x,'subset')
    )

  })

  # extract cross validation specifications-------------------------------------
  cv_spec_l <- map(.x=l,.f=~ {

    pluck(.x,'cv_grid')

  })

  # extract model specifications------------------------------------------------
  m_spec_l <- map(.x=l,.f=~ {

    # apply cartesian grid over list and transpose reduction--------------------
    modelParameterGrid(
      l=pluck(.x,'model_specs')
    )

  })

  # reduce data, cross validation, and model specifications---------------------
  f_spec <- mapply(function(d,c,m) {

    grid_j <- list(
      d[,.SD,.SDcols=-c('data_spec')],
      c,
      m[,.SD,.SDcols=-c('m_spec')]
    ) %>%
      reduceGridList()

    grid_j[,dummy := NULL]

  },
  d=dt_spec_l,
  m=m_spec_l,
  c=cv_spec_l,
  SIMPLIFY=FALSE
  ) %>% rbindlist(use.names=TRUE)

  # bind parameter specification tables-----------------------------------------
  m_spec <- bindParameterSpecs(l=m_spec_l,by='m_hash')
  dt_spec <- bindParameterSpecs(l=dt_spec_l,by='data_hash')

  # create forecast parameter database list-------------------------------------
  pdb <- list(
    f_spec=f_spec,
    d_spec=dt_spec,
    m_spec=m_spec
  )

  cat(
    'Number of data groups:',nrow(dt_spec),'\n',
    'Number of models:',nrow(m_spec),'\n',
    'Number of simulations to run:',nrow(f_spec),
    fill=TRUE
    )

  return(pdb)

}
