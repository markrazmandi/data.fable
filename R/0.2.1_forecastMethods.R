# rolling mean forecast---------------------------------------------------------
rollingMean <- function(data,m_spec,train_start,test_start,test_end) {

  # compute moving average------------------------------------------------------
  data[
    ds %between% c(train_start,test_start-1),
    y_ma := rollapplyr(
      data=y,
      width=m_spec$n,
      fill=NA,
      FUN=mean,
      partial=TRUE
    )
    ]

  # shift moving average and fill forecast values-------------------------------
  data[,yhat := na.locf(
    object=shift(
      x=y_ma,
      n=1,
      fill=0,
      type='lag'
    ),
    na.rm=FALSE
  )]

  # extract future forecast-----------------------------------------------------
  p_f <- data[ds %between% c(test_start,test_end),.(
    ds,
    horizon=rleid(ds),
    y,
    yhat,
    err=yhat-y
  )]

  # compute total forecast error------------------------------------------------
  # p_e <- p_f[,sum(err,na.rm=TRUE)]

  # assign model fit and predictions--------------------------------------------
  l <- list(
    m_fit=data,
    m_pred=p_f
    # m_err=p_e
  )

  return(l)

}

# prophet forecast--------------------------------------------------------------
prophetForecast <- function(data,m_spec,train_start,test_start,test_end) {

  # initialize prophet model----------------------------------------------------
  m <- prophet(
    df=NULL,
    growth=m_spec$main$growth %set% 'linear',
    changepoints=m_spec$main$changepoints,
    n.changepoints=m_spec$main$n_changepoints %set% 25,
    changepoint.range=m_spec$main$changepoint_range %set% 0.8,
    yearly.seasonality=FALSE,
    weekly.seasonality=FALSE,
    daily.seasonality=FALSE,
    holidays=m_spec$main$holidays,
    seasonality.mode=m_spec$main$seasonality_mode %set% 'additive',
    seasonality.prior.scale=m_spec$main$seasonality_prior_scale %set% 10,
    holidays.prior.scale=m_spec$main$holidays_prior_scale %set% 10,
    changepoint.prior.scale=m_spec$main$changepoint_prior_scale %set% 0.5,
    mcmc.samples=m_spec$main$mcmc.samples %set% 0,
    interval.width=m_spec$main$interval_width %set% 0.8,
    uncertainty.samples=m_spec$main$uncertainty_samples %set% 0,
    fit=FALSE
  )

  # recursively add seasonalities-----------------------------------------------
  for (i in seq_along(m_spec$seasonality)) {

    m <- add_seasonality(
      m=m,
      name=names(m_spec$seasonality[i]),
      period=m_spec$seasonality[[i]]$period,
      fourier.order=m_spec$seasonality[[i]]$fourier_order,
      prior.scale=m_spec$seasonality[[i]]$prior_scale,
      mode=m_spec$seasonality[[i]]$mode
    )

  }

  # recursively add regressors--------------------------------------------------
  for (i in seq_along(m_spec$regressor)) {

    if (m_spec$regressor[[i]]$prior_scale!=0) {

      m <- add_regressor(
        m=m,
        name=m_spec$regressor[[i]]$name,
        prior.scale=m_spec$regressor[[i]]$prior_scale,
        standardize=m_spec$regressor[[i]]$standardize %set% 'auto',
        mode=m_spec$regressor[[i]]$mode
      )

    }

  }

  # fit prohpet model on training data------------------------------------------
  m_f <- fit.prophet(
    m=m,
    df=data[ds %between% c(train_start,test_start-1)],
    algorithm=m_spec$optim$algorithm %set% 'LBFGS',
    iter=m_spec$optim$iter %set% 1000
  )

  # fit prophet model on test data----------------------------------------------
  f <- predict(
    m_f,
    data[ds %between% c(test_start,test_end)]
  )

  f$ds <- as.IDate(f$ds)

  # merge actuals to forecast---------------------------------------------------
  f_d <- merge(
    x=data,
    y=f,
    by='ds',
    suffixes=c('.input','.prophet'),
    all.y=TRUE
  )

  # compute error and assign horizon periods------------------------------------
  f_d[,`:=` (
    err=yhat-y,
    horizon=rleid(ds)
  )]

  # compute total forecast error------------------------------------------------
  # p_e <- f_d[,sum(err,na.rm=TRUE)]

  # coerce history parameter to list for speed----------------------------------
  m_f$history <- as.list(m_f$history)

  # add forecast dataset to model parameter list--------------------------------
  m_f$forecast <- as.list(f_d)

  # assign model fit and predictions--------------------------------------------
  l <- list(
    m_fit=m_f,
    m_pred=f_d[,.SD,.SDcols=c('ds','horizon','y','yhat','err')]
    # m_err=p_e
  )

  return(l)

}

# forecast model switch---------------------------------------------------------
forecastModels <- function(m,data,m_spec,train_start,test_start,test_end) {

  switch(
    EXPR=as.character(m),
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
