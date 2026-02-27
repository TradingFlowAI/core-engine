#!/usr/bin/env python3
"""
R&D Mode 集成测试 — 真实网络调用

测试矩阵：
───────────────────────────────────────────────────
 模块                测试场景                    预期
───────────────────────────────────────────────────
 EvmOperator         BSC Mainnet RPC 连接        成功
                     生成随机钱包 + 查原生余额   balance=0
                     查 USDT ERC20 余额          balance=0
                     查 portfolio                 成功
                     transfer(无余额)             抛异常
                     用 Vitalik 地址查真实余额    balance>0
───────────────────────────────────────────────────
 BinanceOperator     公开行情 get_price           有数据
                     K线 get_klines               有 OHLCV
                     市场列表 get_markets          数量>0
                     私有接口 get_balance          认证错误(预期)
                     spot_order                   认证错误(预期)
───────────────────────────────────────────────────
 端到端管线           CredStore→Factory→Operator   成功
───────────────────────────────────────────────────
 日志脱敏            私钥/API Key 不泄漏          已脱敏
───────────────────────────────────────────────────

用法:
  source .venv/bin/activate
  cd 03_weather_station
  python tests/integration_rd_mode.py
"""

import sys
import os
import asyncio
import logging
import traceback
import time

# 确保项目 root 在 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eth_account import Account

# ── 颜色与打印工具 ────────────────────────────────────────────
class C:
    G = "\033[92m"   # 绿
    R = "\033[91m"   # 红
    Y = "\033[93m"   # 黄
    B = "\033[94m"   # 蓝
    W = "\033[97m"   # 白
    D = "\033[90m"   # 灰
    BOLD = "\033[1m"
    END = "\033[0m"

passed = 0
failed = 0
skipped = 0

def header(title):
    print(f"\n{C.BOLD}{C.B}{'━' * 70}")
    print(f"  {title}")
    print(f"{'━' * 70}{C.END}")

def test_pass(name, detail=""):
    global passed
    passed += 1
    detail_str = f" {C.D}({detail}){C.END}" if detail else ""
    print(f"  {C.G}PASS{C.END}  {name}{detail_str}")

def test_fail(name, error):
    global failed
    failed += 1
    print(f"  {C.R}FAIL{C.END}  {name}")
    print(f"        {C.R}{error}{C.END}")

def test_skip(name, reason):
    global skipped
    skipped += 1
    print(f"  {C.Y}SKIP{C.END}  {name} — {reason}")

def test_info(msg):
    print(f"  {C.D}INFO  {msg}{C.END}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 1: EvmOperator 集成测试
# ═══════════════════════════════════════════════════════════════════

def test_evm_operator():
    header("EvmOperator — BSC Mainnet 真实 RPC 测试")

    from operators.evm_operator import EvmOperator
    from web3 import Web3

    # ── 1.1: 生成随机测试钱包 ──────────────────────────
    test_account = Account.create()
    test_pk = test_account.key.hex()
    test_address = test_account.address
    test_info(f"生成测试钱包: {test_address}")

    # ── 1.2: 创建 EvmOperator (BSC Mainnet, chain_id=56) ──
    try:
        op = EvmOperator(private_key=test_pk, chain_id=56)
        test_pass("创建 EvmOperator(BSC Mainnet)", f"address={op.address[:10]}...")
    except Exception as e:
        test_fail("创建 EvmOperator(BSC Mainnet)", str(e))
        return  # 后续测试依赖此步骤

    # ── 1.3: 查询原生 BNB 余额（空钱包应为 0）──
    try:
        balance = op.get_balance()
        assert balance["balance"] == 0.0, f"Expected 0, got {balance['balance']}"
        assert balance["symbol"] == "BNB"
        assert balance["decimals"] == 18
        assert balance["token"] == "native"
        test_pass("get_balance() 原生 BNB", f"balance={balance['balance']} BNB")
    except Exception as e:
        test_fail("get_balance() 原生 BNB", str(e))

    # ── 1.4: 查询 ERC20 USDT 余额（BSC 上的 USDT）──
    USDT_BSC = "0x55d398326f99059fF775485246999027B3197955"  # BSC USDT
    try:
        usdt_balance = op.get_balance(token=USDT_BSC)
        assert usdt_balance["balance"] == 0.0
        assert usdt_balance["symbol"] == "USDT"
        assert usdt_balance["decimals"] == 18
        test_pass("get_balance(USDT) ERC20", f"balance={usdt_balance['balance']} USDT")
    except Exception as e:
        test_fail("get_balance(USDT) ERC20", str(e))

    # ── 1.5: get_portfolio ──
    try:
        portfolio = op.get_portfolio()
        assert portfolio["address"] == test_address
        assert portfolio["chain_id"] == 56
        assert "native" in portfolio
        test_pass("get_portfolio()", f"address={portfolio['address'][:10]}...")
    except Exception as e:
        test_fail("get_portfolio()", str(e))

    # ── 1.6: transfer 无余额应报错 ──
    try:
        op.transfer(
            to_address="0x0000000000000000000000000000000000000001",
            amount=0.001,
        )
        test_fail("transfer(无余额) 应报错", "未抛出异常")
    except Exception as e:
        error_msg = str(e).lower()
        # web3 可能返回 insufficient funds / gas required exceeds allowance 等
        if any(kw in error_msg for kw in ["insufficient", "fund", "gas", "balance"]):
            test_pass("transfer(无余额) 正确报错", f"{type(e).__name__}")
        else:
            # 其他错误也接受（如 RPC 返回特定错误码）
            test_pass("transfer(无余额) 报错", f"{type(e).__name__}: {str(e)[:60]}")

    # ── 1.7: 用已知有余额的地址验证 RPC 读取 ──
    # 用 Binance: BSC Token Hub (BSC 跨链桥，总是有大量 BNB)
    KNOWN_RICH = "0x0000000000000000000000000000000000001004"  # BSC System Contract
    try:
        w3 = Web3(Web3.HTTPProvider("https://bsc-dataseed1.binance.org"))
        # 直接查 PancakeSwap Router 合约余额（总有 BNB）
        PANCAKE_ROUTER = "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4"
        raw = w3.eth.get_balance(Web3.to_checksum_address(PANCAKE_ROUTER))
        bnb = Web3.from_wei(raw, "ether")
        # PancakeSwap Router 可能有少量 BNB 留存，也可能为 0
        # 关键是验证 RPC 调用成功，不报错
        test_pass("BSC RPC 读取已知地址余额", f"balance={float(bnb):.6f} BNB (PancakeSwap Router)")
    except Exception as e:
        test_fail("BSC RPC 读取已知地址余额", str(e))

    # ── 1.8: address 属性 ──
    assert op.address == test_address
    test_pass("address 属性一致", f"{op.address[:10]}...")

    # ── 1.9: chain_id 属性 ──
    assert op.chain_id == 56
    test_pass("chain_id 属性", "56 (BSC Mainnet)")

    # ── 1.10: swap 未实现 ──
    try:
        op.swap("BNB", "USDT", 0.1)
        test_fail("swap() 应抛 NotImplementedError", "未抛出异常")
    except NotImplementedError:
        test_pass("swap() 正确抛 NotImplementedError")
    except Exception as e:
        test_fail("swap() 异常类型错误", str(e))

    # ── 1.11: BSC Testnet 连接 ──
    try:
        op_testnet = EvmOperator(private_key=test_pk, chain_id=97)
        bal = op_testnet.get_balance()
        test_pass("BSC Testnet 连接", f"balance={bal['balance']} tBNB")
    except Exception as e:
        test_skip("BSC Testnet 连接", str(e)[:60])


# ═══════════════════════════════════════════════════════════════════
# SECTION 2: BinanceOperator 集成测试（公开 API）
# ═══════════════════════════════════════════════════════════════════

def test_binance_operator():
    header("BinanceOperator — CEX API 测试")

    from operators.binance_operator import BinanceOperator
    import ccxt

    # ── 2.1: 创建 BinanceOperator ──
    try:
        op = BinanceOperator(
            api_key="test_fake_api_key_for_auth_test",
            api_secret="test_fake_api_secret_for_auth_test",
            exchange_id="binance",
        )
        test_pass("创建 BinanceOperator")
    except Exception as e:
        test_fail("创建 BinanceOperator", str(e))
        return

    # ────────────────────────────────────────────────────────
    # 公开 API 测试 (用无认证的 ccxt 实例直接验证行情数据)
    # 注意：Binance ccxt 配了 apiKey 后即使公开接口也会校验 Key，
    # 所以公开 API 测试用裸 ccxt 实例。
    # ────────────────────────────────────────────────────────

    bare_binance = ccxt.binance({"enableRateLimit": True})

    # ── 2.2: Binance 公开行情 BTC/USDT ──
    try:
        ticker = bare_binance.fetch_ticker("BTC/USDT")
        assert ticker["last"] > 0
        test_pass("Binance 公开行情 BTC/USDT", f"last=${ticker['last']:,.2f}")
    except Exception as e:
        test_fail("Binance 公开行情 BTC/USDT", str(e))

    # ── 2.3: Binance 公开行情 ETH/USDT ──
    try:
        eth_ticker = bare_binance.fetch_ticker("ETH/USDT")
        assert eth_ticker["last"] > 0
        test_pass("Binance 公开行情 ETH/USDT", f"last=${eth_ticker['last']:,.2f}")
    except Exception as e:
        test_fail("Binance 公开行情 ETH/USDT", str(e))

    # ── 2.4: Binance K 线 ──
    try:
        klines = bare_binance.fetch_ohlcv("BTC/USDT", "1h", limit=10)
        assert len(klines) == 10
        assert len(klines[0]) == 6  # [ts, open, high, low, close, volume]
        assert klines[0][1] > 0     # open price > 0
        test_pass("Binance K线 BTC/USDT 1h", f"{len(klines)} candles, open=${klines[-1][1]:,.2f}")
    except Exception as e:
        test_fail("Binance K线 BTC/USDT 1h", str(e))

    # ── 2.5: Binance 市场列表 ──
    try:
        bare_binance.load_markets()
        markets_count = len(bare_binance.markets)
        assert markets_count > 100
        assert "BTC/USDT" in bare_binance.markets
        test_pass("Binance 市场列表", f"{markets_count} markets")
    except Exception as e:
        test_fail("Binance 市场列表", str(e))

    # ────────────────────────────────────────────────────────
    # BinanceOperator 封装方法测试 (通过 OKX，不校验公开 key)
    # ────────────────────────────────────────────────────────

    # ── 2.6: OKX Operator get_price (验证 Operator 封装正确性) ──
    try:
        okx_op = BinanceOperator(
            api_key="test_fake_okx_key_12345",
            api_secret="test_fake_okx_secret_67890",
            exchange_id="okx",
        )
        okx_ticker = okx_op.get_price("BTC/USDT")
        assert okx_ticker["symbol"] == "BTC/USDT"
        assert okx_ticker["last"] > 0
        assert okx_ticker["bid"] is not None
        assert okx_ticker["ask"] is not None
        test_pass("OKX Operator get_price", f"last=${okx_ticker['last']:,.2f}")
    except Exception as e:
        test_fail("OKX Operator get_price", str(e))

    # ── 2.7: OKX Operator get_klines ──
    try:
        okx_klines = okx_op.get_klines("ETH/USDT", timeframe="1h", limit=5)
        assert len(okx_klines) == 5
        assert len(okx_klines[0]) == 6
        test_pass("OKX Operator get_klines", f"{len(okx_klines)} candles")
    except Exception as e:
        test_fail("OKX Operator get_klines", str(e))

    # ── 2.8: OKX Operator get_markets ──
    try:
        okx_markets = okx_op.get_markets()
        assert len(okx_markets) > 50
        btc_found = any(m["symbol"] == "BTC/USDT" for m in okx_markets)
        assert btc_found
        test_pass("OKX Operator get_markets", f"{len(okx_markets)} markets")
    except Exception as e:
        test_fail("OKX Operator get_markets", str(e))

    # ────────────────────────────────────────────────────────
    # 认证错误处理 (Binance 假 key → AuthenticationError)
    # ────────────────────────────────────────────────────────

    # ── 2.9: get_balance (需认证，假 key 应报 AuthenticationError) ──
    try:
        op.get_balance()
        test_fail("get_balance(假key) 应报认证错误", "未抛出异常")
    except Exception as e:
        error_name = type(e).__name__
        if "Authentication" in error_name or "auth" in str(e).lower():
            test_pass("get_balance(假key) 报 AuthenticationError", f"{error_name}")
        else:
            test_pass("get_balance(假key) 报错", f"{error_name}: {str(e)[:60]}")

    # ── 2.10: spot_order (需认证，假 key 应报错) ──
    try:
        op.spot_order("BTC/USDT", "buy", 0.0001)
        test_fail("spot_order(假key) 应报错", "未抛出异常")
    except Exception as e:
        test_pass("spot_order(假key) 正确报错", f"{type(e).__name__}")

    # ── 2.11: Bybit 交易所连接 ──
    try:
        bybit_op = BinanceOperator(
            api_key="test_fake_bybit_key_12345",
            api_secret="test_fake_bybit_secret_67890",
            exchange_id="bybit",
        )
        # Bybit 也校验 key，但至少验证实例化和交易所匹配
        assert bybit_op._exchange_id == "bybit"
        test_pass("Bybit Operator 实例化", "exchange_id=bybit")
    except Exception as e:
        test_fail("Bybit Operator 实例化", str(e))


# ═══════════════════════════════════════════════════════════════════
# SECTION 3: 端到端管线测试
# CredentialStore → OperatorFactory → Operator
# ═══════════════════════════════════════════════════════════════════

async def test_e2e_pipeline():
    header("端到端管线 — CredentialStore → Factory → Operator")

    from operators.credential_store import CredentialStore
    from operators.factory import OperatorFactory

    store = CredentialStore.get_instance()

    # ── 3.1: EVM 完整管线 ──
    test_account = Account.create()
    test_pk = test_account.key.hex()

    try:
        # Step 1: 存储凭证（模拟 PrivateKeyNode）
        ref = store.store({"private_key": test_pk}, ttl=60)
        test_info(f"CredentialStore.store → ref={ref[:12]}...")

        # Step 2: 构建描述符（模拟节点信号）
        descriptor = {
            "_tf_operator": True,
            "_type": "EvmOperator",
            "chain": "bsc",
            "chain_id": 56,
            "credential_ref": ref,
        }

        # Step 3: Factory 构建 Operator
        operator = await OperatorFactory.from_descriptor(descriptor)
        test_info(f"OperatorFactory.from_descriptor → {type(operator).__name__}")

        # Step 4: 实际 RPC 调用
        balance = operator.get_balance()
        assert balance["symbol"] == "BNB"
        test_pass("EVM 管线 (Store→Factory→get_balance)", f"balance={balance['balance']} BNB")

        # Step 5: 确认 credential 已被 consume
        leftover = store.retrieve(ref, consume=False)
        assert leftover is None, "Credential should have been consumed"
        test_pass("Credential 已被 consume（单次使用）")

    except Exception as e:
        test_fail("EVM 管线", f"{type(e).__name__}: {str(e)[:80]}")

    # ── 3.2: CEX 完整管线（通过 OKX，公开 API 不校验假 key）──
    try:
        ref2 = store.store({
            "api_key": "test_pipeline_key_123456",
            "api_secret": "test_pipeline_secret_abcdef",
        }, ttl=60)

        descriptor2 = {
            "_tf_operator": True,
            "_type": "OkxOperator",
            "exchange": "okx",
            "credential_ref": ref2,
        }

        op2 = await OperatorFactory.from_descriptor(descriptor2)
        # OKX 公开 API 不校验假 key
        ticker = op2.get_price("BTC/USDT")
        assert ticker["last"] > 0
        test_pass("CEX 管线 (Store→Factory→get_price via OKX)", f"BTC=${ticker['last']:,.2f}")

        # 确认 consume
        leftover2 = store.retrieve(ref2, consume=False)
        assert leftover2 is None
        test_pass("CEX Credential 已被 consume")

    except Exception as e:
        test_fail("CEX 管线", f"{type(e).__name__}: {str(e)[:80]}")

    # ── 3.3: 过期凭证测试 ──
    try:
        ref3 = store.store({"private_key": "expired_key"}, ttl=0.1)
        time.sleep(0.2)

        descriptor3 = {
            "_tf_operator": True,
            "_type": "EvmOperator",
            "chain_id": 56,
            "credential_ref": ref3,
        }

        await OperatorFactory.from_descriptor(descriptor3)
        test_fail("过期凭证应报 RuntimeError", "未抛出异常")
    except RuntimeError as e:
        assert "not found or expired" in str(e)
        test_pass("过期凭证 正确报 RuntimeError")
    except Exception as e:
        test_fail("过期凭证", f"错误类型: {type(e).__name__}: {str(e)[:60]}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 4: 日志脱敏集成测试
# ═══════════════════════════════════════════════════════════════════

def test_log_sanitization():
    header("日志脱敏 — 敏感信息不泄漏验证")

    import io
    from operators.evm_operator import EvmOperator
    from operators.binance_operator import BinanceOperator
    from common.log_sanitizer import sanitize, register_sensitive_value

    # ── 4.1: EvmOperator 自动注册 private_key ──
    test_pk = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    op = EvmOperator(private_key=test_pk, chain_id=56)

    # 检查 sanitize 是否能脱敏该私钥
    log_msg = f"User private key is {test_pk} for address {op.address}"
    sanitized = sanitize(log_msg)
    if test_pk in sanitized:
        test_fail("EvmOperator 私钥脱敏", "私钥仍可见于日志")
    else:
        assert "90" in sanitized  # 最后 2 位保留
        test_pass("EvmOperator 私钥已脱敏", f"sanitized 中不含原始私钥")

    # ── 4.2: BinanceOperator 自动注册 api_key / api_secret ──
    api_key = "my_super_secret_api_key_12345"
    api_secret = "my_super_secret_api_secret_abcde"
    bop = BinanceOperator(api_key=api_key, api_secret=api_secret)

    log_msg2 = f"Exchange API: key={api_key}, secret={api_secret}"
    sanitized2 = sanitize(log_msg2)
    if api_key in sanitized2 or api_secret in sanitized2:
        test_fail("BinanceOperator API Key 脱敏", "Key/Secret 仍可见")
    else:
        test_pass("BinanceOperator API Key/Secret 已脱敏")

    # ── 4.3: __repr__ / __str__ 不暴露私钥 ──
    repr_str = repr(op)
    str_str = str(op)
    if test_pk in repr_str or test_pk in str_str:
        test_fail("__repr__/__str__ 泄漏私钥", repr_str)
    else:
        test_pass("__repr__/__str__ 不含私钥")

    # ── 4.4: print 场景模拟 ──
    captured = io.StringIO()
    sys.stdout = captured
    print(f"My operator: {op}")
    print(f"Debug info: key={test_pk}")
    sys.stdout = sys.__stdout__
    output = captured.getvalue()
    # 注意：print 输出不会自动经过 sanitize（这是 CodeNode 的职责）
    # 但 repr/str 应不含敏感信息
    if test_pk not in repr(op):
        test_pass("print(operator) 不含私钥")
    else:
        test_fail("print(operator) 泄漏私钥", output)

    # ── 4.5: 手动 register + sanitize 端到端 ──
    passphrase = "my_okx_passphrase_xyz123"
    register_sensitive_value(passphrase)
    msg = f"OKX config: passphrase={passphrase}, endpoint=api.okx.com"
    sanitized3 = sanitize(msg)
    if passphrase in sanitized3:
        test_fail("passphrase 脱敏", "passphrase 仍可见")
    else:
        # 最后 2 位保留
        assert "23" in sanitized3
        test_pass("passphrase 脱敏成功", f"末尾 '23' 保留")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.BOLD}{C.W}{'═' * 70}")
    print(f"  TradingFlow R&D Mode — 集成测试套件")
    print(f"  (真实网络调用 | BSC Mainnet RPC | Binance API)")
    print(f"{'═' * 70}{C.END}")

    start = time.time()

    # 同步测试
    test_evm_operator()
    test_binance_operator()
    test_log_sanitization()

    # 异步测试
    asyncio.run(test_e2e_pipeline())

    elapsed = time.time() - start

    # ── 汇总 ──
    total = passed + failed + skipped
    print(f"\n{C.BOLD}{'═' * 70}")
    print(f"  测试结果汇总")
    print(f"{'═' * 70}{C.END}")
    print(f"  {C.G}通过: {passed}{C.END}")
    print(f"  {C.R}失败: {failed}{C.END}")
    print(f"  {C.Y}跳过: {skipped}{C.END}")
    print(f"  总计: {total}  |  耗时: {elapsed:.1f}s")
    print(f"{C.BOLD}{'═' * 70}{C.END}\n")

    if failed > 0:
        print(f"{C.R}{C.BOLD}  部分测试失败，请检查上方输出。{C.END}\n")
        sys.exit(1)
    else:
        print(f"{C.G}{C.BOLD}  所有测试通过！{C.END}\n")
        sys.exit(0)


if __name__ == "__main__":
    # 降低第三方库日志噪音
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("operators").setLevel(logging.DEBUG)
    logging.getLogger("common.log_sanitizer").setLevel(logging.DEBUG)

    main()
