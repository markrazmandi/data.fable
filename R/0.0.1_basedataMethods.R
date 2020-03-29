# get selected attributes of object---------------------------------------------
getAttrs <- function(obj,attrs=TRUE) {

  attrs <- attributes(x=obj)[attrs]

  if (length(attrs)==1) {

    attrs <- unlist(attrs,recursive=FALSE,use.names=FALSE)

  }

  return(attrs)

}

# set selected attributes of object---------------------------------------------
setAttrs <- function(obj,attrs) {

  iwalk(attrs,~setattr(x=obj,name=.y,value=.x))

}

# load data---------------------------------------------------------------------
loadData <- function(l,label) {

  if (!exists(x=label,where=.GlobalEnv)) {

    dt <- lapply(l,function(j) {

      path <- sub('^.*\\.(.*)$','\\1',j)

      switch(
        EXPR=path,
        'rds'=readRDS(j),
        'csv'=fread(j,na.strings=''),
        'zip'=fread(cmd=paste('unzip -p',j),na.strings='')
      )


    }) %>% rbindlist(use.names=TRUE)

    assign(x=label,value=dt,pos=1L)

  }

}

# build timeseries datatable----------------------------------------------------
buildDatatable <- function(dt,filter=TRUE,key,index,frequency,
                           week_start=getOption('week_start',1L),
                           y,measures=NULL) {

  # drop missing dates----------------------------------------------------------
  if (any(is.na(dt[[index]]))) {

    warning('removing records from <',ds,'> with missing dates')

    dt <- dt[!is.na(get(index))]

  }

  # aggregate date index--------------------------------------------------------
  dt[,ds := as.IDate(
    floor_date(
      x=as.Date(get(index)),
      unit=frequency,
      week_start=week_start
    )
  )]

  # apply filter and sum data over key columns----------------------------------
  dt <- dt[
    eval(filter),
    lapply(.SD,sum,na.rm=TRUE),
    .SDcols=c(y,measures),
    by=c(key,'ds')
    ]

  # coerce keys to factor-------------------------------------------------------
  if (!is.null(key)) {

    dt[,(key) := lapply(.SD,as.factor),.SDcols=key]

  }

  # set model names-------------------------------------------------------------
  setnames(dt,y,'y')

  # set key---------------------------------------------------------------------
  setkeyv(dt,c(key,'ds'))

  # set selected attributes of object-------------------------------------------
  setAttrs(obj=dt,attrs=list(group_by=key,frequency=frequency))

  return(dt)

}

# extract baseline features of timeseries data----------------------------------
baselineFeatures <- function(dt,key=getAttrs(dt,'group_by'),
                             frequency=getAttrs(dt,'frequency'),horizon_length,
                             cv_folds,cv_step,actuals_limit) {

  # aggregate data and duration features by group-------------------------------
  dt_dr <- dt[,.(
    y=round(sum(y,na.rm=TRUE)),
    y_abs=round(sum(abs(y),na.rm=TRUE)),
    y_mean=round(mean(y,na.rm=TRUE)),
    y_med=round(median(y,na.rm=TRUE)),
    y_sd=round(sd(y,na.rm=TRUE)),
    y_abs_sd=round(sd(abs(y),na.rm=TRUE)),
    min_ds=min(ds),
    max_ds=max(ds),
    periods=.N
  ),
  by=key
  ]

  # compute duration------------------------------------------------------------
  dt_dr[,duration := time_length(
    x=interval(
      start=min_ds,
      end=max_ds
      ),
    unit=frequency
    )+1]

  # order dataset---------------------------------------------------------------
  setorder(dt_dr,-y_abs)

  # set selected attributes of object-------------------------------------------
  setAttrs(obj=dt_dr,attrs=list(group_by=key,frequency=frequency))

  # compute cumulative distributions--------------------------------------------
  dt_dr[,y_abs_pct := y_abs/sum(y_abs)]
  dt_dr[,y_abs_cum_pct := cumsum(y_abs_pct)]
  dt_dr[,y_abs_max_pct := y_abs/max(y_abs)]

  # test for current activity---------------------------------------------------
  dt_dr[,active := fifelse(max_ds>as.IDate(actuals_limit)-120,1,0)]

  # test for overall lifecycle duration-----------------------------------------
  # time_length(interval(start=min_ds,end=max_ds),unit='year')<0.9
  dt_dr[,lifecycle := fifelse(duration>=(horizon_length)+(cv_folds*cv_step),1,0)]

  # test for proportional contribution -----------------------------------------
  # | y_abs_pct<=0.001
  dt_dr[,contribution := fifelse(y_abs_max_pct>0.001,1,0)]

  # classify applicable timeseries models---------------------------------------
  dt_dr[active==0,model := 'disqualified']
  dt_dr[active==1 & lifecycle==1 & contribution==1,model := 'sophisticated']
  dt_dr[is.na(model),model := 'naive']

  dt_dr[,model := as.factor(model)]

  print(dt_dr[,.N,model])

  return(dt_dr)

}

# fill date gaps by group and pad missing values--------------------------------
fillGaps <- function(dt,date_limit,test_end=NULL,
                     frequency=getAttrs(dt,'frequency'),
                     key=getAttrs(dt,'group_by'),
                     measures,fill_fun=0L,full=FALSE) {

  if (!full | is.null(key)) {

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

  } else if (full & !is.null(key)) {

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

  } else {

    stop('error in full fill arg <',full,'> and key arg <',key,'>')

  }

  # merge date padding to dataset-----------------------------------------------
  dt <- merge(x=dt,y=ds,all=TRUE)

  # pad missing values----------------------------------------------------------
  lapply(c('y',measures),function(j) {

    dt[is.na(get(j)) & ds<date_limit,(j) := fill_fun,by=key]

  }) %>% invisible()

  # set selected attributes of object-------------------------------------------
  setAttrs(obj=dt,attrs=list(group_by=key,frequency=frequency))

  return(dt)

}
