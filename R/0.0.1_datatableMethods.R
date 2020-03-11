# build timeseries datatable from input parameters------------------------------
buildDatatable <- function(dt,key,index,frequency,week_start,y,measures) {

  # aggregate date index--------------------------------------------------------
  dt[,(index) := as.IDate(
    floor_date(
      x=as.Date(get(index)),
      unit=frequency,
      week_start=week_start
    )
  )]

  # sum data over key columns---------------------------------------------------
  dt <- dt[,lapply(.SD,sum,na.rm=TRUE),.SDcols=c(y,measures),by=c(index,key)]

  # coerce keys to factor-------------------------------------------------------
  dt[,(key) := lapply(.SD,as.factor),.SDcols=key]

  # set index and actuals names-------------------------------------------------
  setnames(dt,c(index,y),c('ds','y'))

  # set key---------------------------------------------------------------------
  setkeyv(dt,c(key,'ds'))

  return(dt)

}

# extract baseline features of timeseries data----------------------------------
baselineFeatures <- function(dt,key,frequency) {

  # aggregate data and duration features by group-------------------------------
  dt_dr <- dt[,.(
    y=round(sum(y,na.rm=TRUE)),
    y_mean=round(mean(y,na.rm=TRUE)),
    y_med=round(median(y,na.rm=TRUE)),
    y_sd=round(sd(y,na.rm=TRUE)),
    min_ds=min(ds),
    max_ds=max(ds),
    periods=.N
  ),
  by=key
  ]

  # compute duration------------------------------------------------------------
  dt_dr[,duration := time_length(interval(min_ds,max_ds),unit=frequency)+1]

  # order dataset---------------------------------------------------------------
  setorder(dt_dr,-y)

  # compute cumulative distributions--------------------------------------------
  dt_dr[,y_pct := y/sum(y)]
  dt_dr[,y_cum_pct := cumsum(y_pct)]

  # classify naive and timeseries applicable groups-----------------------------
  dt_dr[

    # Duration of cross validation + 6 months
    time_length(interval(start=min_ds,end=max_ds),unit='year')<0.9

    # exception: 1000 forecasts that have uniform distribution
    # % of max value
    # drop forecasts up to 3% of total value (relative drop)
    | abs(y_pct) <= 0.001,
    model := 'naive'
    ]

  dt_dr[max_ds<=Sys.Date()-120,model := 'disqualified']

  dt_dr[is.na(model),model := 'sophisticated']

  return(dt_dr)

}

# fill date gaps by group and pad missing values--------------------------------
fillGaps <- function(dt,date_limit,test_end=NULL,frequency,key,
                     measures,fill_fun=0L,full=FALSE) {

  if (!full) {

    # fill date gaps by group---------------------------------------------------
    ds <- dt[,.(
      ds=as.IDate(
        seq.Date(
          from=min(ds),
          to=as.Date(ifelse(is.null(test_end),max(ds),test_end)),
          by=frequency
        )
      )
    ),by=key]

    # reset key-----------------------------------------------------------------
    setkeyv(ds,c(key,'ds'))

  } else if (full) {

    # full panel fill missing dates across groups-------------------------------
    sd <- dt[,.(unique(.SD),dummy=0L),.SDcols=key]

    ds <- as.IDate(
      seq.Date(
        from=min(dt$ds),
        to=as.Date(ifelse(is.null(test_end),max(dt$ds),test_end)),
        by=frequency
      )
    ) %>% data.table(ds=.,dummy=0L)

    ds <- merge(x=sd,y=ds,allow.cartesian=TRUE) %>%
      .[,dummy := NULL]

  }

  # merge date padding to dataset-----------------------------------------------
  dt <- merge(x=dt,y=ds,all=TRUE)

  # pad missing values----------------------------------------------------------
  lapply(c('y',measures),function(j) {

    dt[is.na(get(j)) & ds<date_limit,(j) := fill_fun,by=key]

  }) %>% invisible()

  return(dt)

}
