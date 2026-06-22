"""plot_curves.py — small-multiples of the certified zero-support recoveries' dose-response curves.
Each panel: min-max-normalized output (log10) vs input (log10), titled in/out + atlas label + local C_S.
Shows the bandpass plateau the atlas's plurality label + K=16 hid.
    python3 plot_curves.py --curves /tmp/recovered_curves.json --out doc/figures/zerosupport_recoveries.pdf
"""
import argparse, json, math, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curves", default="/tmp/recovered_curves.json")
    ap.add_argument("--out", default="doc/figures/zerosupport_recoveries.pdf")
    ap.add_argument("--cols", type=int, default=5)
    a = ap.parse_args()
    C = json.load(open(a.curves))
    C = [c for c in C if c.get("u") and c.get("ylog") and (c.get("C_S_local") or 0) >= 0.6]
    C.sort(key=lambda c: (c["input_symbol"], c["observe_species"]))
    n = len(C); cols = a.cols; rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(2.0 * cols, 1.6 * rows), squeeze=False)
    for i, c in enumerate(C):
        ax = axes[i // cols][i % cols]
        u = np.array(c["u"], float); y = np.array(c["ylog"], float)
        m = np.isfinite(u) & np.isfinite(y); u, y = u[m], y[m]
        yn = (y - y.min()) / (y.max() - y.min()) if y.max() > y.min() else y * 0
        ax.plot(u, yn, lw=1.3, color="#1f77b4")
        ax.set_title(f"{c['input_symbol']}→{c['observe_species']}\nC_S={c.get('C_S_local','?')}", fontsize=6)
        ax.tick_params(labelsize=5); ax.set_ylim(-0.05, 1.05)
    for j in range(n, rows * cols):
        axes[j // cols][j % cols].axis("off")
    fig.suptitle(f"Certified zero-support bandpass recoveries (n={n}): atlas shape_support=0, "
                 f"but a robust kd makes a bandpass plateau", fontsize=8)
    fig.supxlabel("input (log10)", fontsize=7); fig.supylabel("output (min-max norm.)", fontsize=7)
    fig.tight_layout(rect=[0.02, 0.02, 1, 0.96])
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    fig.savefig(a.out, dpi=150)
    print(f"wrote {a.out}  ({n} panels)")


if __name__ == "__main__":
    main()
