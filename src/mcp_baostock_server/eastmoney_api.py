"""
EastMoney / Sina data backend for the MCP stock-data server.

Why this exists: the original backend used baostock, whose data socket
(www.baostock.com:10030) is unreachable from networks outside mainland China
(connection times out). EastMoney's push2 / datacenter HTTPS endpoints and
Sina's realtime quote endpoint are reachable internationally, so this module
re-implements the same method surface as the old BaoStockAPI using them.

Method names and signatures intentionally mirror BaoStockAPI so server.py only
needs to swap the import. Return values are plain JSON-serializable
dict / list[dict] (not DataFrames).
"""

import re
import time
import httpx
from typing import Optional, Dict, Any, List

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# baostock adjustflag -> eastmoney fqt
_FQT = {"1": "2", "2": "1", "3": "0"}          # 1后复权 2前复权 3不复权
# baostock frequency -> eastmoney klt
_KLT = {"d": "101", "w": "102", "m": "103",
        "5": "5", "15": "15", "30": "30", "60": "60"}


def _f(v) -> Optional[float]:
    """Parse an eastmoney numeric field; '-' / '' / None -> None."""
    if v in (None, "-", "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class EastMoneyAPI:
    def __init__(self, timeout: float = 12.0):
        self._client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": _UA},
            follow_redirects=True,
        )

    def _get(self, url: str, params: Optional[dict] = None,
             headers: Optional[dict] = None, attempts: int = 4) -> httpx.Response:
        """GET with retries — EastMoney's push2 endpoints intermittently drop
        keep-alive connections (RemoteProtocolError). Retry on transient
        network errors with short backoff."""
        last: Optional[Exception] = None
        for i in range(attempts):
            try:
                r = self._client.get(url, params=params, headers=headers)
                r.raise_for_status()
                return r
            except (httpx.RemoteProtocolError, httpx.ConnectError,
                    httpx.ReadError, httpx.ReadTimeout, httpx.ConnectTimeout,
                    httpx.PoolTimeout, httpx.RemoteProtocolError) as e:
                last = e
                # drop possibly-stale pooled connections before retrying
                try:
                    self._client.close()
                except Exception:
                    pass
                self._client = httpx.Client(
                    timeout=12.0, headers={"User-Agent": _UA},
                    follow_redirects=True,
                )
                time.sleep(0.4 * (i + 1))
        raise RuntimeError(f"东财/新浪请求失败（重试{attempts}次后）: {last}")

    # ------------------------------------------------------------------ #
    # code helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _split(code: str):
        """('sz','002049') from sz.002049 / sz002049 / 002049 (best-effort)."""
        if not code:
            raise ValueError("代码不能为空")
        c = re.sub(r"\s+", "", code).lower()
        m = re.match(r"^(sh|sz|bj)\.?(\d{6})$", c)
        if m:
            return m.group(1), m.group(2)
        m = re.match(r"^(\d{6})$", c)
        if m:
            num = m.group(1)
            if num.startswith(("600", "601", "603", "605", "688", "689",
                               "510", "511", "512", "513", "515", "518")):
                return "sh", num
            if num.startswith(("000", "001", "002", "003", "300", "301",
                               "159")):
                return "sz", num
            if num.startswith("399"):
                return "sz", num
            if num.startswith(("4", "8")):
                return "bj", num
            return "sh", num
        raise ValueError(f"无效代码格式: {code}（示例 sh.600000 / sz.002049）")

    @classmethod
    def _secid(cls, code: str) -> str:
        ex, num = cls._split(code)
        market = {"sh": "1", "sz": "0", "bj": "0"}[ex]
        return f"{market}.{num}"

    @classmethod
    def _canon(cls, code: str) -> str:
        ex, num = cls._split(code)
        return f"{ex}.{num}"

    @staticmethod
    def _quarter_enddate(year: str, quarter: str) -> str:
        return {"1": f"{year}-03-31", "2": f"{year}-06-30",
                "3": f"{year}-09-30", "4": f"{year}-12-31"}[str(quarter)]

    # ------------------------------------------------------------------ #
    # realtime snapshot (Tencent qt.gtimg.cn)
    # ------------------------------------------------------------------ #
    # NOTE: EastMoney push2 (push2.eastmoney.com/api/qt/stock/get) 302-redirects
    # / drops connections for this IP (anti-bot rate limit), so the snapshot
    # uses Tencent's gtimg endpoint instead — it returns price/PE(TTM)/PB/market
    # cap in one GBK string and is reliable internationally. Field indices below
    # are positional in the ~-delimited payload (verified 2026-05).
    def _snapshot(self, code: str) -> Dict[str, Any]:
        ex, num = self._split(code)
        url = f"https://qt.gtimg.cn/q={ex}{num}"
        r = self._get(url, headers={"Referer": "https://gu.qq.com"})
        r.encoding = "gbk"
        m = re.search(r'="([^"]*)"', r.text)
        if not m or not m.group(1):
            raise RuntimeError(f"腾讯无快照数据: {code}")
        a = m.group(1).split("~")

        def g(i):
            return a[i] if len(a) > i else None

        def yi(i):  # 亿元 -> 元
            x = _f(g(i))
            return round(x * 1e8, 2) if x is not None else None

        return {
            "code": self._canon(code),
            "code_name": g(1),
            "close": _f(g(3)),
            "preclose": _f(g(4)),
            "open": _f(g(5)),
            "high": _f(g(33)),
            "low": _f(g(34)),
            "volume": _f(g(6)),                 # 手
            "amount": (lambda x: round(x * 1e4, 2) if x is not None else None)(_f(g(37))),  # 万元 -> 元
            "turn": _f(g(38)),                  # 换手率 %
            "pctChg": _f(g(32)),                # 涨跌幅 %
            "peTTM": _f(g(39)),                 # 市盈率 TTM
            "pbMRQ": _f(g(46)),                 # 市净率
            "psTTM": None,
            "totalMarketCap": yi(45),           # 总市值
            "circMarketCap": yi(44),            # 流通市值
            "industry": None,                   # 腾讯不直接提供行业
            "datetime": g(30),
        }

    def _sina_quote(self, code: str) -> Dict[str, Any]:
        """Backup realtime quote (clean, unscaled values)."""
        ex, num = self._split(code)
        url = f"https://hq.sinajs.cn/list={ex}{num}"
        r = self._get(url, headers={"Referer": "https://finance.sina.com.cn"})
        r.encoding = "gbk"
        txt = r.text
        m = re.search(r'="([^"]*)"', txt)
        if not m or not m.group(1):
            raise RuntimeError(f"新浪无行情: {code}")
        a = m.group(1).split(",")
        return {
            "code": self._canon(code),
            "code_name": a[0],
            "open": _f(a[1]), "preclose": _f(a[2]), "close": _f(a[3]),
            "high": _f(a[4]), "low": _f(a[5]),
            "volume": _f(a[8]), "amount": _f(a[9]),
            "date": a[30] if len(a) > 30 else None,
            "time": a[31] if len(a) > 31 else None,
        }

    # ------------------------------------------------------------------ #
    # public surface (mirrors BaoStockAPI)
    # ------------------------------------------------------------------ #
    def get_stock_basic(self, code: str) -> Dict[str, Any]:
        s = self._snapshot(code)
        return {
            "code": s["code"],
            "code_name": s["code_name"],
            "industry": s.get("industry"),
            "close": s.get("close"),
            "totalMarketCap": s.get("totalMarketCap"),
            "circMarketCap": s.get("circMarketCap"),
            "source": "tencent",
        }

    # K-line via Tencent ifzq (EastMoney push2his rate-limits this IP).
    _TX_PERIOD = {"d": "day", "w": "week", "m": "month"}
    _TX_FQ = {"1": "hfq", "2": "qfq", "3": ""}   # 1后复权 2前复权 3不复权

    def get_history_k_data(self, code: str,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           frequency: str = "d",
                           adjustflag: str = "3") -> Dict[str, Any]:
        ex, num = self._split(code)
        period = self._TX_PERIOD.get(str(frequency))
        if period is None:
            return {"code": self._canon(code), "frequency": frequency,
                    "count": 0, "data": [],
                    "note": "腾讯日/周/月线 only(分钟线暂不支持)",
                    "source": "tencent"}
        fq = self._TX_FQ.get(str(adjustflag), "")
        beg = start_date or "1990-01-01"
        end = end_date or "2050-01-01"
        param = f"{ex}{num},{period},{beg},{end},640,{fq}"
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        r = self._get(url, params={"param": param})
        node = (((r.json() or {}).get("data") or {}).get(f"{ex}{num}")) or {}
        # key is fq+period (qfqday/hfqday) or plain period (day) for 不复权
        series = (node.get(f"{fq}{period}") or node.get(period)
                  or node.get(f"qfq{period}") or [])
        rows: List[Dict[str, Any]] = []
        for c in series:
            # [date, open, close, high, low, volume, ...]
            rows.append({
                "date": c[0], "open": _f(c[1]), "close": _f(c[2]),
                "high": _f(c[3]), "low": _f(c[4]), "volume": _f(c[5]),
            })
        return {"code": self._canon(code), "frequency": frequency,
                "adjustflag": adjustflag, "count": len(rows),
                "data": rows, "source": "tencent"}

    def get_valuation_data(self, code: str,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           frequency: str = "d") -> Dict[str, Any]:
        """Latest valuation snapshot (peTTM/pbMRQ). NOTE: returns the latest
        live snapshot, not a historical per-day series."""
        s = self._snapshot(code)
        return {
            "code": s["code"], "code_name": s["code_name"],
            "close": s["close"], "peTTM": s["peTTM"], "pbMRQ": s["pbMRQ"],
            "psTTM": s["psTTM"], "totalMarketCap": s["totalMarketCap"],
            "note": "latest live snapshot (date-range ignored)",
            "source": "tencent",
        }

    # ---- financial report (datacenter RPT_LICO_FN_CPD / 业绩报表) ---- #
    def _perf_rows(self, code: str, page_size: int = 8) -> List[Dict[str, Any]]:
        _, num = self._split(code)
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "reportName": "RPT_LICO_FN_CPD", "columns": "ALL",
            "filter": f'(SECURITY_CODE="{num}")',
            "sortColumns": "REPORTDATE", "sortTypes": "-1",
            "pageSize": str(page_size), "pageNumber": "1",
        }
        r = self._get(url, params=params,
                      headers={"Referer": "https://data.eastmoney.com/"})
        res = (r.json() or {}).get("result") or {}
        return res.get("data") or []

    @staticmethod
    def _shape_perf(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "code": row.get("SECURITY_CODE"),
            "code_name": row.get("SECURITY_NAME_ABBR"),
            "report_date": (row.get("REPORTDATE") or "")[:10],
            "quarter": row.get("QDATE"),
            "report_type": row.get("DATATYPE"),
            "notice_date": (row.get("NOTICE_DATE") or "")[:10],
            "revenue": _f(row.get("TOTAL_OPERATE_INCOME")),          # 元
            "net_profit": _f(row.get("PARENT_NETPROFIT")),           # 归母, 元
            "eps": _f(row.get("BASIC_EPS")),
            "deduct_eps": _f(row.get("DEDUCT_BASIC_EPS")),
            "roe": _f(row.get("WEIGHTAVG_ROE")),                     # %
            "gross_margin": _f(row.get("XSMLL")),                    # %
            "ocf_per_share": _f(row.get("MGJYXJJE")),
            "bps": _f(row.get("BPS")),
            "revenue_yoy": _f(row.get("YSTZ")),                      # %
            "net_profit_yoy": _f(row.get("SJLTZ")),                  # %
            "revenue_qoq": _f(row.get("YSHZ")),                      # %
            "net_profit_qoq": _f(row.get("SJLHZ")),                  # %
        }

    def _find_quarter(self, code: str, year: str, quarter: str) -> Dict[str, Any]:
        target = self._quarter_enddate(year, quarter)
        rows = self._perf_rows(code, page_size=16)
        for row in rows:
            if (row.get("REPORTDATE") or "").startswith(target):
                return self._shape_perf(row)
        raise RuntimeError(f"未找到 {self._canon(code)} {year}Q{quarter} 业绩数据")

    def get_profit_data(self, code: str, year, quarter) -> Dict[str, Any]:
        return self._find_quarter(code, str(year), str(quarter))

    def get_growth_data(self, code: str, year, quarter) -> Dict[str, Any]:
        d = self._find_quarter(code, str(year), str(quarter))
        return {k: d[k] for k in (
            "code", "code_name", "report_date", "quarter", "report_type",
            "net_profit", "eps", "revenue",
            "revenue_yoy", "net_profit_yoy", "revenue_qoq", "net_profit_qoq",
        )}

    def get_operation_data(self, code: str, year, quarter) -> Dict[str, Any]:
        """Turnover ratios are not exposed by the eastmoney performance
        report; returns what the report does carry plus a note."""
        d = self._find_quarter(code, str(year), str(quarter))
        d["note"] = ("operation/turnover ratios (应收/存货周转) not available "
                     "from eastmoney 业绩报表; only summary metrics returned")
        return d

    def get_index_data(self, code: str,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       frequency: str = "d") -> Dict[str, Any]:
        return self.get_history_k_data(code, start_date, end_date,
                                       frequency, adjustflag="3")

    def get_dividend_data(self, code: str, year: str = "") -> Dict[str, Any]:
        _, num = self._split(code)
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "reportName": "RPT_SHAREBONUS_DET", "columns": "ALL",
            "filter": f'(SECURITY_CODE="{num}")',
            "sortColumns": "NOTICE_DATE", "sortTypes": "-1",
            "pageSize": "20", "pageNumber": "1",
        }
        r = self._get(url, params=params,
                      headers={"Referer": "https://data.eastmoney.com/"})
        res = (r.json() or {}).get("result") or {}
        rows = res.get("data") or []
        out = []
        for row in rows:
            y = (row.get("REPORT_DATE") or "")[:4]
            if year and y != str(year):
                continue
            out.append({
                "code": row.get("SECURITY_CODE"),
                "report_year": y,
                "report_date": (row.get("REPORT_DATE") or "")[:10],
                "plan": row.get("IMPL_PLAN_PROFILE"),
                "cash_per_share_tax": _f(row.get("PRETAX_BONUS_RIT")),
                "ex_date": (row.get("EX_DIVIDEND_DATE") or "")[:10],
                "register_date": (row.get("EQUITY_RECORD_DATE") or "")[:10],
            })
        return {"code": self._canon(code), "count": len(out),
                "data": out, "source": "eastmoney"}

    def get_industry_classified(self, code: str = "") -> Dict[str, Any]:
        if not code:
            return {"note": "全市场行业分类不支持；请传入单只代码", "data": []}
        s = self._snapshot(code)
        return {"code": s["code"], "code_name": s["code_name"],
                "industry": s.get("industry"), "source": "eastmoney"}
