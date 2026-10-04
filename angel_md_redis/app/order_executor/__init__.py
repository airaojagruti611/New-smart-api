"""
Module 14 — Order Executor / Trade Entry Engine (DECISION.md §7).

Executes an ICARE-APPROVED trade: fresh validation, bid/ask + depth, a capped
limit-order ladder (never a market order), quantity MIN(), liquidity slicing,
partial-fill continue/stop, timeout, kill switch, charges and slippage report.

Pure package (no I/O). Redis + broker wiring lives in run_order_executor.py.
"""
