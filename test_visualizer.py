from pathlib import Path

from visualizer import plot_trade_prices


def test_plot_trade_prices(tmp_path: Path) -> None:
    trades = [
        {"timestamp": 1, "price": 10},
        {"timestamp": 2, "price": 20},
    ]
    file_path = tmp_path / "plot.png"
    assert plot_trade_prices(trades, file_path)
    assert file_path.exists() and file_path.stat().st_size > 0
