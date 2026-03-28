# ============================================================
# EFIM20040 Financial Data Coursework
# Final Script - Task I: Portfolio Analysis and Improvement
#
# Student PERMNOs: 90988, 75714, 46690
# Target SIC codes: 2895, 3663, 8059
#
# Chosen strategy:
# - Universe: All US common stocks in SIC 2895, 3663, 8059
# - Signal: Gross Profitability = (Revenue - COGS) / Total Assets
# - Accounting frequency: Annual
# - Lag: 6 months
# - Screen: Top 25% by gross profitability
# - Weighting: Minimum Variance
# - Fallback: Equal weight if min-variance fails
# - Rebalance: Monthly
#
# Benchmarks required by the brief:
# 1. Client's original 3-stock value-weighted portfolio
# 2. 1/N equal-weight benchmark
#
# Graphs included:
# 1. Client original portfolio cumulative growth
# 2. Strategy cumulative growth
# 3. 1/N benchmark cumulative growth
# 4. Combined cumulative growth comparison
# ============================================================
install.packages("quadprog")

# ============================================================
# 1. LOAD REQUIRED PACKAGES
# ============================================================

library(tidyverse)
library(lubridate)
library(RPostgres)
library(quadprog)


# ============================================================
# 2. STRATEGY AND SAMPLE PARAMETERS
# These are the key assumptions used throughout the script.
# ============================================================

assigned_permnos <- c(90988, 75714, 46690)
target_sic_codes <- c(2895, 3663, 8059)

sample_start <- as.Date("2014-01-01")
sample_end_exclusive <- as.Date("2024-02-01")   # captures Jan 2024 month-end

gp_screen_pct <- 0.25
fundamental_lag_months <- 6
minvar_lookback_months <- 36
min_history_obs <- 24

assigned_permnos
target_sic_codes
sample_start
sample_end_exclusive


# ============================================================
# 3. CONNECT TO WRDS
# Replace the username and password fields with your own.
# ============================================================

wrds <- dbConnect(
  Postgres(),
  host = "wrds-pgdata.wharton.upenn.edu",
  dbname = "wrds",
  port = 9737,
  sslmode = "require",
  user = "aryendra23",
  password = "pubxYc-movsy3-dunqeh"
)


# ============================================================
# 4. IDENTIFY THE ASSIGNED FIRMS
# Pull the latest available firm identity and SIC information
# for your three assigned PERMNOs.
# ============================================================

query_names <- "
select
    permno,
    ticker,
    comnam,
    siccd,
    namedt,
    nameendt
from crsp_a_stock.dsenames
where permno in (90988, 75714, 46690)
order by permno, namedt
"

assigned_names <- dbGetQuery(wrds, query_names)

assigned_names_clean <- assigned_names |>
  mutate(
    namedt = as.Date(namedt),
    nameendt = as.Date(nameendt)
  ) |>
  arrange(permno, desc(namedt))

assigned_latest <- assigned_names_clean |>
  group_by(permno) |>
  slice(1) |>
  ungroup()

assigned_latest


# ============================================================
# 5. PULL MONTHLY RETURNS FOR THE SAME-INDUSTRY UNIVERSE
# We retrieve all US common stocks in SIC 2895, 3663, and 8059.
#
# Filters used:
# - shrcd in (10,11): US common stocks
# - exchcd in (1,2,3): NYSE, AMEX, NASDAQ
#
# Monthly data comes from CRSP msf and firm attributes are
# matched from dsenames using the valid name date range.
# ============================================================

get_universe_query <- function(sic_code) {
  paste0("
    select
        m.permno,
        m.date,
        m.ret,
        m.prc,
        m.shrout,
        n.ticker,
        n.comnam,
        n.siccd,
        n.shrcd,
        n.exchcd
    from crsp_a_stock.msf as m
    inner join crsp_a_stock.dsenames as n
        on m.permno = n.permno
       and m.date >= n.namedt
       and m.date <= coalesce(n.nameendt, date '9999-12-31')
    where n.siccd = ", sic_code, "
      and n.shrcd in (10, 11)
      and n.exchcd in (1, 2, 3)
      and m.date >= '2014-01-01'
      and m.date < '2024-02-01'
    order by m.permno, m.date
  ")
}

universe_2895 <- dbGetQuery(wrds, get_universe_query(2895))
universe_3663 <- dbGetQuery(wrds, get_universe_query(3663))
universe_8059 <- dbGetQuery(wrds, get_universe_query(8059))

universe_returns_raw <- bind_rows(universe_2895, universe_3663, universe_8059)


# ============================================================
# 6. CLEAN THE UNIVERSE RETURNS DATA
# - remove duplicate stock-month rows if they appear
# - convert columns to proper formats
# - create market capitalisation
# ============================================================

universe_returns <- universe_returns_raw |>
  distinct(permno, date, .keep_all = TRUE) |>
  mutate(
    date = as.Date(date),
    ret = as.numeric(ret),
    prc = as.numeric(prc),
    shrout = as.numeric(shrout),
    siccd = as.integer(siccd),
    shrcd = as.integer(shrcd),
    exchcd = as.integer(exchcd),
    mktcap = abs(prc) * shrout
  ) |>
  arrange(date, permno)

universe_returns


# ============================================================
# 7. BUILD THE ASSIGNED-STOCK SUBSET
# This subset will be used for the client's original portfolio.
# ============================================================

assigned_returns <- universe_returns |>
  filter(permno %in% assigned_permnos)

assigned_returns


# ============================================================
# 8. PULL ANNUAL FUNDAMENTALS FOR THE UNIVERSE
# Signal chosen:
# Gross Profitability = (Revenue - COGS) / Total Assets
#
# We link Compustat annual fundamentals to CRSP PERMNOs using
# the CRSP-Compustat Merged (CCM) link table.
# ============================================================

universe_permnos <- universe_returns |>
  distinct(permno) |>
  pull(permno)

permno_sql_list <- paste(universe_permnos, collapse = ", ")

fundamentals_query <- paste0("
    select
        l.lpermno as permno,
        f.gvkey,
        f.datadate,
        f.fyear,
        f.sale,
        f.cogs,
        f.at
    from comp.funda as f
    inner join crsp_a_ccm.ccmxpf_linktable as l
        on f.gvkey = l.gvkey
    where l.linktype in ('LU', 'LC')
      and l.linkprim in ('P', 'C')
      and (l.linkdt is null or f.datadate >= l.linkdt)
      and (l.linkenddt is null or f.datadate <= l.linkenddt)
      and f.indfmt = 'INDL'
      and f.datafmt = 'STD'
      and f.popsrc = 'D'
      and f.consol = 'C'
      and f.sale is not null
      and f.cogs is not null
      and f.at is not null
      and f.at > 0
      and l.lpermno in (", permno_sql_list, ")
      and f.datadate >= '2012-01-01'
      and f.datadate < '2024-01-01'
    order by l.lpermno, f.datadate
")

fundamentals_raw <- dbGetQuery(wrds, fundamentals_query)


# ============================================================
# 9. CLEAN FUNDAMENTALS AND CREATE THE INVESTABLE SIGNAL
# We apply a 6-month lag to avoid look-ahead bias.
# ============================================================

fundamentals_signal <- fundamentals_raw |>
  mutate(
    permno = as.integer(permno),
    datadate = as.Date(datadate),
    sale = as.numeric(sale),
    cogs = as.numeric(cogs),
    at = as.numeric(at),
    gross_profitability = (sale - cogs) / at,
    signal_available_date = datadate %m+% months(fundamental_lag_months)
  ) |>
  filter(is.finite(gross_profitability)) |>
  arrange(permno, signal_available_date)

fundamentals_signal


# ============================================================
# 10. HELPER FUNCTION: MINIMUM-VARIANCE WEIGHTS
# This function computes long-only minimum-variance weights
# subject to weights summing to 1.
#
# If the covariance matrix cannot be used, the function returns
# an empty vector and the main strategy falls back to equal
# weights.
# ============================================================

solve_min_var_weights <- function(hist_returns_mat) {
  
  if (ncol(hist_returns_mat) == 0) {
    return(numeric(0))
  }
  
  # Drop columns with too few usable observations
  enough_history <- colSums(!is.na(hist_returns_mat)) >= min_history_obs
  hist_returns_mat <- hist_returns_mat[, enough_history, drop = FALSE]
  
  if (ncol(hist_returns_mat) == 0) {
    return(numeric(0))
  }
  
  if (ncol(hist_returns_mat) == 1) {
    w <- 1
    names(w) <- colnames(hist_returns_mat)
    return(w)
  }
  
  sigma <- cov(hist_returns_mat, use = "pairwise.complete.obs")
  
  # Remove invalid or zero-variance columns
  valid_cols <- is.finite(diag(sigma)) & diag(sigma) > 0 & colSums(is.na(sigma)) == 0
  sigma <- sigma[valid_cols, valid_cols, drop = FALSE]
  
  if (ncol(sigma) == 0) {
    return(numeric(0))
  }
  
  if (ncol(sigma) == 1) {
    w <- 1
    names(w) <- colnames(sigma)
    return(w)
  }
  
  # Small ridge term to improve numerical stability
  sigma <- sigma + diag(1e-6, ncol(sigma))
  
  dvec <- rep(0, ncol(sigma))
  Amat <- cbind(rep(1, ncol(sigma)), diag(ncol(sigma)))
  bvec <- c(1, rep(0, ncol(sigma)))
  
  qp_solution <- tryCatch(
    solve.QP(
      Dmat = 2 * sigma,
      dvec = dvec,
      Amat = Amat,
      bvec = bvec,
      meq = 1
    )$solution,
    error = function(e) NULL
  )
  
  if (is.null(qp_solution)) {
    return(numeric(0))
  }
  
  names(qp_solution) <- colnames(sigma)
  qp_solution
}


# ============================================================
# 11. CLIENT'S ORIGINAL VALUE-WEIGHTED PORTFOLIO
# This implements the client's original passive allocation
# across the three assigned stocks.
# ============================================================

client_original_return <- assigned_returns |>
  filter(!is.na(ret), !is.na(mktcap), mktcap > 0) |>
  group_by(date) |>
  mutate(weight = mktcap / sum(mktcap, na.rm = TRUE)) |>
  summarise(
    portfolio_ret = sum(weight * ret, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(date)

client_original_return


# ============================================================
# 12. 1/N BENCHMARK
# This is the equal-weight benchmark across the full same-
# industry universe, rebalanced monthly.
# ============================================================

benchmark_1n_return <- universe_returns |>
  filter(!is.na(ret)) |>
  group_by(date) |>
  summarise(
    portfolio_ret = mean(ret, na.rm = TRUE),
    n_stocks = n(),
    .groups = "drop"
  ) |>
  arrange(date)

benchmark_1n_return


# ============================================================
# 13. STRATEGY CONSTRUCTION
# Monthly rebalancing process:
# - At month t, use information available up to month t-1
# - Apply the latest lagged annual profitability signal
# - Keep the top 25% of stocks by gross profitability
# - Estimate covariance using the trailing 36 months of returns
# - Apply minimum-variance weights
# - If optimisation fails, fall back to equal weights
# ============================================================

all_months <- sort(unique(universe_returns$date))
strategy_results <- list()

for (i in 2:length(all_months)) {
  
  current_date <- all_months[i]
  info_date <- all_months[i - 1]
  
  hist_start_index <- max(1, i - minvar_lookback_months)
  hist_dates <- all_months[hist_start_index:(i - 1)]
  
  # Stocks investable at the information date
  investable_universe <- universe_returns |>
    filter(date == info_date, !is.na(mktcap), mktcap > 0) |>
    select(permno, siccd)
  
  # Latest available lagged profitability signal as of info_date
  latest_signal <- fundamentals_signal |>
    filter(signal_available_date <= info_date) |>
    group_by(permno) |>
    slice_max(order_by = signal_available_date, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(permno, gross_profitability, signal_available_date)
  
  candidates <- investable_universe |>
    inner_join(latest_signal, by = "permno")
  
  if (nrow(candidates) == 0) {
    next
  }
  
  n_to_keep <- max(2, ceiling(gp_screen_pct * nrow(candidates)))
  
  selected_by_profitability <- candidates |>
    arrange(desc(gross_profitability)) |>
    slice_head(n = n_to_keep)
  
  # Current month returns for selected stocks
  current_month_returns <- universe_returns |>
    filter(date == current_date, !is.na(ret)) |>
    select(permno, ret)
  
  selected_current <- selected_by_profitability |>
    inner_join(current_month_returns, by = "permno")
  
  if (nrow(selected_current) == 0) {
    next
  }
  
  # Historical returns used for covariance estimation
  history_panel <- universe_returns |>
    filter(
      date %in% hist_dates,
      permno %in% selected_current$permno
    ) |>
    select(date, permno, ret)
  
  hist_matrix_tbl <- history_panel |>
    pivot_wider(names_from = permno, values_from = ret) |>
    arrange(date)
  
  hist_matrix <- hist_matrix_tbl |>
    select(-date) |>
    as.matrix()
  
  if (ncol(hist_matrix) == 0) {
    next
  }
  
  weight_method <- "minimum_variance"
  weights_vec <- solve_min_var_weights(hist_matrix)
  
  if (length(weights_vec) == 0) {
    weight_method <- "equal_weight_fallback"
    weights_tbl <- selected_current |>
      distinct(permno) |>
      mutate(weight = 1 / n())
  } else {
    weights_tbl <- tibble(
      permno = as.integer(names(weights_vec)),
      weight = as.numeric(weights_vec)
    )
    
    # If optimisation dropped some assets, only keep the weighted set
    selected_current <- selected_current |>
      inner_join(weights_tbl, by = "permno")
  }
  
  if (weight_method == "equal_weight_fallback") {
    strategy_month <- selected_current |>
      distinct(permno, ret) |>
      mutate(weight = 1 / n()) |>
      summarise(
        date = current_date,
        portfolio_ret = sum(weight * ret, na.rm = TRUE),
        n_holdings = n(),
        weight_method = "equal_weight_fallback"
      )
  } else {
    strategy_month <- selected_current |>
      summarise(
        date = current_date,
        portfolio_ret = sum(weight * ret, na.rm = TRUE),
        n_holdings = n(),
        weight_method = "minimum_variance"
      )
  }
  
  strategy_results[[length(strategy_results) + 1]] <- strategy_month
}

strategy_return <- bind_rows(strategy_results) |>
  arrange(date)

strategy_return


# ============================================================
# 14. PREPARE CUMULATIVE GROWTH SERIES
# Cumulative growth is clearer than noisy monthly return lines
# for showing comparative performance over time.
# ============================================================

client_growth <- client_original_return |>
  arrange(date) |>
  mutate(growth = cumprod(1 + portfolio_ret))

strategy_growth <- strategy_return |>
  arrange(date) |>
  mutate(growth = cumprod(1 + portfolio_ret))

benchmark_1n_growth <- benchmark_1n_return |>
  arrange(date) |>
  mutate(growth = cumprod(1 + portfolio_ret))


# ============================================================
# 15. ALIGN THE THREE SERIES FOR COMBINED COMPARISON
# The strategy series may start later than the benchmarks
# because it needs historical data for optimisation.
# ============================================================

combined_growth <- client_growth |>
  select(date, client_original = growth) |>
  inner_join(
    strategy_growth |>
      select(date, strategy = growth),
    by = "date"
  ) |>
  inner_join(
    benchmark_1n_growth |>
      select(date, benchmark_1n = growth),
    by = "date"
  ) |>
  pivot_longer(
    cols = -date,
    names_to = "portfolio",
    values_to = "growth"
  )


# ============================================================
# 16. GRAPH 1 - CLIENT ORIGINAL VALUE-WEIGHTED PORTFOLIO
# ============================================================

ggplot(client_growth, aes(x = date, y = growth)) +
  geom_line(linewidth = 0.9, color = "black") +
  labs(
    title = "Client Original Value-Weighted Portfolio",
    x = "Date",
    y = "Cumulative Growth of £1"
  ) +
  theme_minimal(base_size = 13)


# ============================================================
# 17. GRAPH 2 - STRATEGY PORTFOLIO
# ============================================================

ggplot(strategy_growth, aes(x = date, y = growth)) +
  geom_line(linewidth = 0.9, color = "blue") +
  labs(
    title = "Strategy Portfolio: Gross Profitability + Minimum Variance",
    x = "Date",
    y = "Cumulative Growth of £1"
  ) +
  theme_minimal(base_size = 13)


# ============================================================
# 18. GRAPH 3 - 1/N BENCHMARK
# ============================================================

ggplot(benchmark_1n_growth, aes(x = date, y = growth)) +
  geom_line(linewidth = 0.9, color = "red") +
  labs(
    title = "1/N Equal-Weight Benchmark",
    x = "Date",
    y = "Cumulative Growth of £1"
  ) +
  theme_minimal(base_size = 13)


# ============================================================
# 19. GRAPH 4 - COMBINED COMPARISON GRAPH
# Different colours are used to make the comparison clear.
# ============================================================

ggplot(combined_growth, aes(x = date, y = growth, color = portfolio)) +
  geom_line(linewidth = 1.0) +
  scale_color_manual(
    values = c(
      "client_original" = "black",
      "strategy" = "blue",
      "benchmark_1n" = "red"
    ),
    labels = c(
      "client_original" = "Client Original Value-Weighted",
      "strategy" = "Strategy Portfolio",
      "benchmark_1n" = "1/N Benchmark"
    )
  ) +
  labs(
    title = "Cumulative Growth Comparison",
    x = "Date",
    y = "Cumulative Growth of £1",
    color = "Portfolio"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "bottom",
    legend.title = element_text(size = 11),
    legend.text = element_text(size = 10)
  )


# ============================================================
# 20. OPTIONAL PERFORMANCE SUMMARY TABLE
# This is useful for interpretation and for your report.
# ============================================================

performance_summary <- bind_rows(
  client_original_return |>
    summarise(
      portfolio = "Client Original Value-Weighted",
      mean_monthly_return = mean(portfolio_ret, na.rm = TRUE),
      volatility = sd(portfolio_ret, na.rm = TRUE),
      sharpe_ratio = mean(portfolio_ret, na.rm = TRUE) / sd(portfolio_ret, na.rm = TRUE)
    ),
  strategy_return |>
    summarise(
      portfolio = "Strategy Portfolio",
      mean_monthly_return = mean(portfolio_ret, na.rm = TRUE),
      volatility = sd(portfolio_ret, na.rm = TRUE),
      sharpe_ratio = mean(portfolio_ret, na.rm = TRUE) / sd(portfolio_ret, na.rm = TRUE)
    ),
  benchmark_1n_return |>
    summarise(
      portfolio = "1/N Benchmark",
      mean_monthly_return = mean(portfolio_ret, na.rm = TRUE),
      volatility = sd(portfolio_ret, na.rm = TRUE),
      sharpe_ratio = mean(portfolio_ret, na.rm = TRUE) / sd(portfolio_ret, na.rm = TRUE)
    )
)

performance_summary


# ============================================================
# 21. DISCONNECT FROM WRDS
# ============================================================

dbDisconnect(wrds)