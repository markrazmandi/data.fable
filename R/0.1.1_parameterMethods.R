# default return values if null-------------------------------------------------
'%set%' <- function(x,y) {

  par <- ifelse(is.null(x),y,x)

  return(par)

}

# subset data according to model class------------------------------------------
dataModelClass <- function(dt,dtc,filter,group_by) {

  dt <- merge(
    x=dt,
    y=dtc[eval(filter),.SD,.SDcols=group_by],
    by=group_by,
    all.y=TRUE
  )

  if (any(is.na(dt$model))) {

    stop('not all *',class,' models merged to data')

  }

  return(dt)

}

# subset data groups for granularity--------------------------------------------
dataGroupByGrid <- function(dt,group_by,incl_data=FALSE) {

  if (incl_data) {

    dt[,.(data=.(.SD),dummy=0L),by=c(group_by)]

  } else {

    dt[,.(dummy=0L),by=c(group_by)]

  }

}

# subset nested data for modeling periods---------------------------------------
sdNested <- function(dt,incl_data=FALSE) {

  if (incl_data) {

    dt[,data := .(
      .(data[[1]][ds %between% c(train_start,test_end)])
    ),by=seq_len(nrow(dt))]

  }

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
addPeriods <- function(date,freq,x) {

  date <- as.Date(date)

  switch(
    EXPR=freq,
    month=date+months(x-1),
    week=date+weeks(x-1),
    day=date+days(x-1)
  )

}

# create cross validation window grid-------------------------------------------
crossValidationGrid <- function(
  frequency,
  horizon_length,
  week_start,
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
    x=horizon_length
  )

  # construct cross validaiton table--------------------------------------------
  dt <- data.table(
    train_start=train_start_folds,
    test_start=test_start_folds,
    test_end=test_end_folds,
    horizon_length=horizon_length,
    frequency=frequency,
    week_start=week_start,
    dummy=0L
  )

  if (frequency!='week') {

    dt[,week_start := NULL]

  }

  return(dt)

}

# sort list elements and discard null elements----------------------------------
compactList <- function(l,decreasing=FALSE) {

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
imapGridList <- function(l,decreasing=FALSE) {

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
modelParameterGrid <- function(l,decreasing=FALSE) {

  # recursive cartesian product creation----------------------------------------
  imapGridList(l) %>%
    imap(~ {

      # rename model specifications column--------------------------------------
      setnames(.x,.y,'m_spec')

      # assign model names hash model specifications----------------------------
      .x[,`:=` (
        m=.y,
        m_hash=md5(as.character(m_spec))
      )]

    }) %>%
    rbindlist(fill=TRUE)

}

# timeseries forecast parameter grid--------------------------------------------
forecastParameterGrid <- function(l,dt,dt_dr,cv_grid,group_by,incl_data) {

  gdb <- lapply(l,function(j) {

    # subset data according to model class--------------------------------------
    dt <- dataModelClass(
      dt=dt,
      dtc=dt_dr,
      filter=j$filter,
      group_by=group_by
    )

    # subset data groups for granularity----------------------------------------
    d_grid <- dataGroupByGrid(
      dt=dt,
      group_by=group_by,
      incl_data=incl_data
    )

    # cartesian join data groups and cross validation windows-------------------
    d_cv_grid <- list(d_grid,cv_grid) %>%
      reduceGridList()

    # subset nested data for modeling periods-----------------------------------
    d_cv_grid <- sdNested(
      dt=d_cv_grid,
      incl_data=incl_data
    )

    # recursively apply cartesian grid over list and transpose reduction--------
    m_grid <- modelParameterGrid(j$model)

    # cartesian join tables within the same depth-------------------------------
    gdb_j <- list(d_cv_grid,m_grid) %>%
      reduceGridList()

    gdb_j[,dummy := NULL]

    return(gdb_j)

  }) %>% rbindlist(use.names=TRUE)

  return(gdb)

}
