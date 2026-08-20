# Alpaca crypto setup and launch runbook

This integration is paper-first. The runtime can monitor Alpaca crypto around
the clock, but strategy-generated live order submission remains disabled until
the runtime controls, `KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED=true`, and the
separate `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED=true` gate all permit it.

## 1. Open and verify the account

1. Confirm that your country of legal residence appears in Alpaca's current
   [supported crypto regions](https://alpaca.markets/support/what-regions-support-cryptocurrency-trading).
   Citizenship, residence, tax status, and account eligibility are different
   questions; answer Alpaca's application exactly as requested.
2. Create an Alpaca Trading API account and complete identity, address, tax,
   sanctions, and crypto disclosures. Wait until both the account and crypto
   status show active.
3. Turn on multi-factor authentication and secure the email account used for
   Alpaca. Do not share API secrets in chat, source control, screenshots, logs,
   or shell history.
4. Create **paper** API keys first. Paper and live use different endpoints and
   key pairs. Store the key and secret in the deployment secret manager.

## 2. Fund the live account only after paper validation

Do not move money for the paper phase. When ready for live validation:

1. In the Alpaca dashboard, open the transfer/deposit flow and copy the current
   USD wire instructions. Do not reuse instructions from a document or old
   transfer—the beneficiary bank and reference fields must come from your own
   account screen.
2. From Chase, send a small USD test wire from an account in the same legal name
   as the Alpaca account. Include Alpaca's exact memo/reference. Alpaca's
   [international funding guidance](https://alpaca.markets/support/international-use-fund-account)
   directs international customers to USD wire funding.
3. Wait for the deposit to be credited and verify available cash in Alpaca.
   Do not enable automation based only on Chase showing the wire as sent.
4. Review the current [crypto fee schedule](https://alpaca.markets/support/crypto-maker-taker-gmt-faq),
   withdrawal rules, and local tax/reporting duties. Crypto assets are not
   protected like SIPC securities or FDIC bank deposits.

## 3. Configure paper mode

Use paper keys in a secret manager or a local ignored `.env`; never commit them.

```bash
export KAI_EXEC_RUNTIME_MODE=paper
export KAI_EXEC_ALPACA_API_KEY='<paper-key>'
export KAI_EXEC_ALPACA_API_SECRET='<paper-secret>'
export KAI_EXEC_ALPACA_CRYPTO_MONITORING_ENABLED=true
export KAI_EXEC_ALPACA_CRYPTO_ALLOWED_SYMBOLS='BTC/USD,ETH/USD'
export KAI_EXEC_ALPACA_CRYPTO_MAX_ORDER_NOTIONAL_USD=100
export KAI_EXEC_ALPACA_CRYPTO_STRATEGY_ENABLED=true
export KAI_EXEC_ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED=false
export KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED=false
export KAI_EXEC_ALPACA_US_EQUITY_MONITORING_ENABLED=true
export KAI_EXEC_ALPACA_US_EQUITY_ALLOWED_SYMBOLS='SPY,QQQ'
export KAI_EXEC_ALPACA_US_EQUITY_STRATEGY_ENABLED=true
export KAI_EXEC_ALPACA_US_EQUITY_STRATEGY_LIVE_ENABLED=false
```

Apply migrations and start the API as documented in the repository README.
Then verify:

```bash
curl http://127.0.0.1:8010/runtime/alpaca-crypto
curl http://127.0.0.1:8010/runtime/status
```

The Alpaca endpoint should become `healthy`, return fresh prices, and show
`live_order_submission_enabled: false`.

If the existing KIS process runs in live mode, do not change its global runtime
mode to test Alpaca paper keys. Use `start-alpaca-paper.sh` on port 8011 with
the separate `var/alpaca_paper_exec.db` database. The launcher keeps the global
KIS autonomous coordinator disabled so KIS monitoring cannot read or mutate
Alpaca paper positions. Run KIS US paper testing separately with
`start-kis-us-paper.sh` on port 8012 and `var/kis_us_paper_exec.db`.

## 4. Automated paper strategy

The isolated paper launcher enables `crypto_trend_v1` by default. It trades
only `BTC/USD` and `ETH/USD`, stays long or in cash, and uses only fully closed
UTC daily bars:

- Entry requires close above EMA200, EMA50 above EMA200, and positive 90-day
  return.
- Eligible assets use inverse 30-day volatility allocation, a 10% portfolio
  volatility target, a 60% invested ceiling, BTC/ETH caps of 40%/30%, and at
  least 40% cash.
- Position value is further capped so the configured protective-stop loss is
  at most 0.25% of account equity.
- Strategy-owned base quantity is persisted per symbol, recomputed from
  strategy-attributed fills, and capped down by current broker truth during
  reconciliation. Manual Alpaca buys or fills from other order origins do not
  increase the sleeve, and automated rebalance, protective, and force-exit
  sells are capped to the strategy-owned quantity.
- Normal allocation runs after 00:10 UTC once per closed bar. Rebalances below
  the greater of $25 or 5% of the sleeve are skipped.
- A 3x ATR protective threshold, capped at 12% distance, trails upward. Fresh
  bid quotes are checked every monitoring cycle.
- Daily 1%, weekly 3%, and peak 8% drawdowns block new entries while allowing
  risk-reducing sells.
- Quotes older than 60 seconds, spreads above 100 bps, unresolved symbol
  orders, missing increments, or incomplete bars fail closed.

The default sleeve is $1,000 and the per-order cap is $100. Change these only
through environment configuration and keep paper/live deployments separate.
The status endpoint exposes quote health, risk state, the last daily decision,
and protective actions:

```bash
curl http://127.0.0.1:8011/runtime/alpaca-crypto
curl http://127.0.0.1:8011/runtime/alpaca-us-equity
```

## 5. Paper buy and sell validation

Resume the runtime deliberately, then submit uniquely identified small paper
orders. Reusing an idempotency key returns the original order instead of
submitting a duplicate.

```bash
curl -X POST http://127.0.0.1:8010/controls/resume \
  -H 'content-type: application/json' \
  -d '{"actor":"operator","source":"manual","reason":"Alpaca paper validation"}'

curl -X POST http://127.0.0.1:8010/orders \
  -H 'content-type: application/json' \
  -d '{
    "idempotency_key":"alpaca-paper-buy-001",
    "provider":"alpaca",
    "asset_class":"crypto",
    "venue":"alpaca",
    "symbol":"BTC/USD",
    "side":"buy",
    "order_type":"market",
    "time_in_force":"gtc",
    "exchange":"ALPACA",
    "quote_notional":"25.00"
  }'

curl -X POST http://127.0.0.1:8010/orders \
  -H 'content-type: application/json' \
  -d '{
    "idempotency_key":"alpaca-paper-sell-001",
    "provider":"alpaca",
    "asset_class":"crypto",
    "venue":"alpaca",
    "symbol":"BTC/USD",
    "side":"sell",
    "order_type":"market",
    "time_in_force":"gtc",
    "exchange":"ALPACA",
    "base_quantity":"0.0001"
  }'
```

Validate order state, fractional fills, positions, pause, restart recovery, and
weekend operation. Paper behavior does not reproduce all live liquidity,
slippage, latency, market impact, or fees.

## 6. Live activation gate

Live activation is an operational change, not only a config edit:

1. Run paper monitoring continuously for at least seven days, including a
   weekend and a service restart.
2. Verify duplicate submissions, rejected orders, partial fills, stale quotes,
   API errors, pause, and kill-switch behavior.
3. Create live API keys and replace the paper keys only in the live deployment
   secret store.
4. Set `KAI_EXEC_RUNTIME_MODE=live`, keep the notional cap small, and leave
   `KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED=false` for a read-only live soak.
5. After account/price/position reconciliation is healthy, explicitly set
   `KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED=true`, restart, resume controls, and
   place one minimal manual order. Confirm broker and local fill/position state.
6. Keep `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED=false` throughout the
   live read-only and manual-order soak. Enabling it is the final, separate
   promotion action after the paper validation period.

The strategy worker creates deterministic order intents, but it cannot bypass
the guarded order service, durable audit records, runtime pause/kill switch,
symbol allowlist, per-order cap, or the two live-trading gates.

## 7. Simultaneous three-market operation

The launch-managed layout keeps the markets isolated by broker and mode:

- Korean equities: KIS live runtime on port 8010, with its existing KRX/NXT
  schedule, monitoring, controls, and strategy pipeline.
- U.S. equities: Alpaca paper SPY/QQQ worker on port 8011. It monitors and
  reconciles continuously, but ordinary decisions and `day` orders run only
  when Alpaca's U.S. market clock reports open.
- KIS U.S. equities: KIS paper worker on port 8012 with its own
  `var/kis_us_paper_exec.db` database and KIS US live submission disabled.
- Crypto: Alpaca paper BTC/USD and ETH/USD worker on port 8011, with continuous
  risk monitoring and one normal decision per closed UTC daily bar.

U.S.-equity and crypto strategies have different versions, allowlists,
idempotency keys, decisions, positions, and live-enable gates. A slow or closed
U.S. market does not pause crypto, and the isolated Alpaca paper process does
not change the live KIS runtime configuration or run KIS monitoring over
Alpaca-owned paper state.
