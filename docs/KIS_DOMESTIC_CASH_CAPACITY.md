# KIS domestic cash-capacity inquiry

`KisBrokerAdapter.get_domestic_cash_capacity` is an optional, read-only adapter
method for a configured KIS account, a six-digit symbol, a positive `Decimal`
limit price, and exactly `KRX`. Mock runtime and other provider/venue scopes
fail before HTTP. It uses the existing client, credentials, timeout and throttle;
it never authenticates, requests a hash key, or submits an order.

The request is `GET /uapi/domestic-stock/v1/trading/inquire-psbl-order` with live
TR `TTTC8908R` or paper TR `VTTC8908R`. It binds configured `CANO` and
`ACNT_PRDT_CD`, `PDNO`, `ORD_UNPR`, `ORD_DVSN=00`,
`CMA_EVLU_AMT_ICLD_YN=N`, and `OVRS_ICLD_YN=N`. The official sample returns one
`output` object and uses no continuation cursor. Unexpected continuation fails
closed rather than producing capacity from an incomplete response.

The frozen `DomesticCashCapacity` binds provider, runtime mode, configured account
reference, symbol, venue, currency, requested price, the four parsed broker
fields, and a UTC receipt time. Its `capacity_id` is SHA-256 of canonical JSON
containing that request scope, those raw fields and receipt time. It is an
observation identifier, not proof of atomic broker state or a reservation.

| Response field | Returned field | Meaning and validation |
| --- | --- | --- |
| `ord_psbl_cash` | `orderable_cash_krw` | Broker-labelled orderable cash; finite and nonnegative |
| `nrcvb_buy_amt` | `no_misu_buying_amount_krw` | Amount without 미수; finite and nonnegative |
| `nrcvb_buy_qty` | `no_misu_buying_quantity` | Quantity without 미수; finite, nonnegative, whole shares |
| `psbl_qty_calc_unpr` | `quantity_calculation_price` | Broker calculation price; finite, nonnegative, positive when quantity is positive |

Missing or malformed fields and provider failures raise `BrokerAdapterError`.
Zero capacity is valid. The method never substitutes `max_buy_amt`,
`max_buy_qty`, collateral, assets, or the generic account-snapshot cash parser.
The caller applies its own cost-aware quantity ceiling and current local
reservation accounting under order serialization.

The official documentation identifies `nrcvb_*` for no-미수 use and `max_*` for
미수 use. It warns that `ORD_DVSN=00` quantity does not reflect the security's
margin rate and directs full possible-quantity inquiries to market type `01`
or the matching special order type. Consequently this limit inquiry is a
conditional capacity ceiling, not full orderability or an execution guarantee.
The public repository does not prove strict settled NET cash, all credit-source
exclusions, fee deductions, or reservation semantics. Excluding CMA valuation and
overseas amounts does not resolve those gaps. The existing account/agent behavior
is unchanged.

Primary references:

- [KIS inquiry request, no-미수 fields and order-type caveat](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_psbl_order/inquire_psbl_order.py)
- [KIS response field labels](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_psbl_order/chk_inquire_psbl_order.py)
- [KIS legacy Y/N and order-code comments](https://github.com/koreainvestment/open-trading-api/blob/main/legacy/Sample01/kis_domstk.py)
- [KIS shared GET transport](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/kis_auth.py)
