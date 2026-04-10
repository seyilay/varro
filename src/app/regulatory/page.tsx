import Sidebar from "@/components/Sidebar";
import { X } from "lucide-react";

const watchlist = ["GOM", "BOEM", "RRC TX", "Fieldwood-successor"];

const alerts = [
  { agency: "BOEM", date: "Mar 28, 2026", title: "BOEM Proposes Amendments to 2024 Offshore Financial Assurance Rule", summary: "Proposed rule would increase financial assurance requirements for offshore operators with $6.9B in new bonding requirements.", affected: 14, severity: "warn" },
  { agency: "BSEE", date: "Mar 25, 2026", title: "NTL Update: Well Control Equipment Testing Requirements", summary: "New notice-to-lessees updates testing frequency requirements for BOP equipment on all active wells.", affected: 8, severity: "info" },
  { agency: "EPA", date: "Mar 22, 2026", title: "ECHO Enforcement Action: GOM Operator Violations", summary: "EPA issues enforcement notice for methane emissions violations at three Gulf of Mexico facilities.", affected: 3, severity: "danger" },
  { agency: "RRC TX", date: "Mar 20, 2026", title: "New P&A Bond Requirements for Permian Basin", summary: "Railroad Commission updates bonding requirements for inactive wells in the Permian Basin region.", affected: 0, severity: "info" },
];

const deadlines = [
  { date: "Apr 15", api: "177-054-xxxxx", requirement: "BOEM Bond Supplementation", status: "OVERDUE" },
  { date: "Jun 01", api: "177-054-xxxxx", requirement: "BSEE Well Integrity Report", days: 63 },
  { date: "Sep 30", api: "177-054-xxxxx", requirement: "RRC TX P&A Order", days: 183 },
];

export default function Regulatory() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center justify-between px-6">
          <div>
            <span className="text-zinc-200 text-sm">Regulatory Tracker</span>
            <span className="text-zinc-500 text-xs ml-2">Monitoring BOEM · BSEE · EPA · RRC TX · IOGCC</span>
          </div>
          <button className="px-4 py-2 border border-[#3a3a44] text-zinc-200 text-sm rounded-lg hover:bg-[#2a2a32]">
            Export Compliance Report
          </button>
        </header>

        <div className="p-6">
          {/* Watchlist */}
          <div className="flex items-center gap-2 mb-6">
            <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">WATCHLIST:</span>
            {watchlist.map((item) => (
              <span key={item} className="flex items-center gap-1 px-2 py-1 bg-[#222228] border border-[#3a3a44] rounded-lg text-sm text-zinc-300">
                {item}
                <X className="w-3 h-3 text-zinc-500 cursor-pointer hover:text-zinc-300" />
              </span>
            ))}
            <button className="px-2 py-1 border border-dashed border-[#3a3a44] rounded-lg text-sm text-zinc-500 hover:border-indigo-500 hover:text-indigo-400">
              + Add Watch
            </button>
          </div>

          <div className="grid grid-cols-3 gap-6">
            {/* Alerts */}
            <div className="col-span-2 space-y-4">
              <div className="text-xs text-zinc-500 uppercase tracking-wider font-medium">RECENT ALERTS</div>
              {alerts.map((alert, i) => (
                <div key={i} className={`bg-[#222228] border-l-2 ${
                  alert.severity === 'danger' ? 'border-rose-400' :
                  alert.severity === 'warn' ? 'border-amber-400' : 'border-indigo-400'
                } rounded-r-lg p-4`}>
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-xs font-medium text-indigo-400 bg-indigo-500/10 px-1.5 py-0.5 rounded">{alert.agency}</span>
                    <span className="text-xs text-zinc-500">{alert.date}</span>
                  </div>
                  <h3 className="text-sm font-medium text-zinc-200 mb-1">{alert.title}</h3>
                  <p className="text-sm text-zinc-400 mb-3">{alert.summary}</p>
                  <div className="flex items-center gap-4">
                    {alert.affected > 0 && (
                      <span className="text-xs text-amber-400">{alert.affected} assets affected</span>
                    )}
                    <a href="#" className="text-xs text-indigo-400 hover:text-indigo-300 font-medium">Read More →</a>
                    <a href="#" className="text-xs text-indigo-400 hover:text-indigo-300 font-medium">Export →</a>
                  </div>
                </div>
              ))}
            </div>

            {/* Deadlines */}
            <div>
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">UPCOMING DEADLINES</div>
              <div className="bg-[#222228] border border-[#3a3a44] rounded-lg divide-y divide-[#3a3a44]">
                {deadlines.map((dl, i) => (
                  <div key={i} className="p-3">
                    <div className="flex items-center justify-between mb-1">
                      <span className={`font-mono text-sm ${dl.status === 'OVERDUE' ? 'text-rose-400' : 'text-zinc-200'}`}>{dl.date}</span>
                      {dl.status === 'OVERDUE' ? (
                        <span className="text-xs text-rose-400 bg-rose-500/10 px-1.5 py-0.5 rounded">⚠ OVERDUE</span>
                      ) : (
                        <span className="text-xs text-zinc-500 font-mono">{dl.days} days</span>
                      )}
                    </div>
                    <div className="text-xs text-indigo-400 font-mono mb-1">{dl.api}</div>
                    <div className="text-xs text-zinc-400">{dl.requirement}</div>
                  </div>
                ))}
              </div>
              <a href="#" className="text-xs text-indigo-400 hover:text-indigo-300 mt-3 block font-medium">View All Deadlines →</a>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
