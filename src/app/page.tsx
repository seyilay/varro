import Sidebar from "@/components/Sidebar";
import { Bell, Search, User } from "lucide-react";

const kpis = [
  { label: "TOTAL ARO EXPOSURE (P50)", value: "$142.3M", color: "text-indigo-400" },
  { label: "TOTAL ARO EXPOSURE (P90)", value: "$198.7M", color: "text-zinc-100" },
  { label: "ASSETS TRACKED", value: "247", color: "text-zinc-100" },
  { label: "DELINQUENT / AT RISK", value: "12", color: "text-rose-400" },
];

const topRiskAssets = [
  { api: "177-054-12345-0000", name: "Well A-42", basin: "GOM", p50: "$2.1M", p90: "$3.4M", confidence: "MEDIUM", status: "DELINQUENT" },
  { api: "177-054-67890-0001", name: "Platform B-7", basin: "GOM", p50: "$1.8M", p90: "$4.1M", confidence: "LOW", status: "IDLE" },
  { api: "177-054-11111-0002", name: "Well C-15", basin: "GOM", p50: "$1.4M", p90: "$2.2M", confidence: "HIGH", status: "PRODUCING" },
  { api: "177-054-22222-0003", name: "Well D-23", basin: "GOM", p50: "$980K", p90: "$1.6M", confidence: "MEDIUM", status: "IDLE" },
  { api: "177-054-33333-0004", name: "Platform E-1", basin: "GOM", p50: "$3.2M", p90: "$5.8M", confidence: "LOW", status: "DELINQUENT" },
];

const alerts = [
  { agency: "BOEM", date: "Mar 28", title: "BOEM Proposes Amendments to 2024 Offshore Financial Assurance Rule", severity: "warn" },
  { agency: "BSEE", date: "Mar 25", title: "NTL Update: Well Control Equipment Testing Requirements", severity: "info" },
  { agency: "EPA", date: "Mar 22", title: "ECHO Enforcement Action: GOM Operator Violations", severity: "danger" },
  { agency: "RRC TX", date: "Mar 20", title: "New P&A Bond Requirements for Permian Basin", severity: "info" },
];

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    DELINQUENT: "bg-rose-500/10 text-rose-400 border-rose-500/20",
    IDLE: "bg-amber-500/10 text-amber-400 border-amber-500/20",
    PRODUCING: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
  };
  return (
    <span className={`px-2 py-0.5 text-xs font-medium uppercase border rounded ${colors[status] || colors.IDLE}`}>
      {status}
    </span>
  );
}

function ConfidenceBadge({ level }: { level: string }) {
  const colors: Record<string, string> = {
    HIGH: "text-emerald-400",
    MEDIUM: "text-amber-400",
    LOW: "text-rose-400",
  };
  return <span className={`text-xs font-medium ${colors[level]}`}>{level}</span>;
}

export default function Dashboard() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      
      <main className="flex-1 overflow-auto">
        {/* Top Bar */}
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center justify-between px-6">
          <div className="flex items-center gap-2 text-sm text-zinc-400">
            <span className="text-zinc-200">Dashboard</span>
          </div>
          <div className="flex items-center gap-4">
            <button className="p-2 text-zinc-400 hover:text-zinc-200 hover:bg-[#2a2a32] rounded">
              <Search className="w-5 h-5" />
            </button>
            <button className="p-2 text-zinc-400 hover:text-zinc-200 hover:bg-[#2a2a32] rounded relative">
              <Bell className="w-5 h-5" />
              <span className="absolute top-1 right-1 w-2 h-2 bg-rose-500 rounded-full" />
            </button>
            <button className="w-8 h-8 bg-indigo-500/20 rounded-full flex items-center justify-center">
              <User className="w-4 h-4 text-indigo-400" />
            </button>
          </div>
        </header>

        {/* Content */}
        <div className="p-6">
          {/* KPI Strip */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            {kpis.map((kpi) => (
              <div key={kpi.label} className="bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
                <div className="text-xs text-zinc-500 uppercase tracking-wider mb-2 font-medium">{kpi.label}</div>
                <div className={`text-2xl font-semibold ${kpi.color}`}>{kpi.value}</div>
              </div>
            ))}
          </div>

          {/* Main Content Grid */}
          <div className="grid grid-cols-3 gap-6 mb-6">
            {/* Chart Area */}
            <div className="col-span-2 bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">ARO EXPOSURE OVER TIME (P50 / P90)</div>
              <div className="h-64 bg-[#2a2a32] rounded-lg flex items-center justify-center">
                <span className="text-zinc-500 text-sm">Chart visualization</span>
              </div>
            </div>

            {/* Alerts */}
            <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">REGULATORY ALERTS</div>
              <div className="space-y-3">
                {alerts.map((alert, i) => (
                  <div key={i} className={`border-l-2 pl-3 py-2 ${
                    alert.severity === 'danger' ? 'border-rose-400' :
                    alert.severity === 'warn' ? 'border-amber-400' : 'border-indigo-400'
                  }`}>
                    <div className="flex items-center gap-2 mb-1">
                      <span className="text-xs font-medium text-indigo-400 bg-indigo-500/10 px-1.5 py-0.5 rounded">{alert.agency}</span>
                      <span className="text-xs text-zinc-500">{alert.date}</span>
                    </div>
                    <p className="text-sm text-zinc-300 line-clamp-2">{alert.title}</p>
                  </div>
                ))}
              </div>
              <a href="/regulatory" className="text-xs text-indigo-400 hover:text-indigo-300 mt-4 block font-medium">View All Alerts →</a>
            </div>
          </div>

          {/* Top Risk Assets Table */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg">
            <div className="px-4 py-3 border-b border-[#3a3a44]">
              <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">TOP RISK ASSETS</span>
            </div>
            <table className="w-full">
              <thead>
                <tr className="text-left text-xs text-zinc-500 uppercase tracking-wider">
                  <th className="px-4 py-3 font-medium">API #</th>
                  <th className="px-4 py-3 font-medium">Well Name</th>
                  <th className="px-4 py-3 font-medium">Basin</th>
                  <th className="px-4 py-3 text-right font-medium">ARO P50</th>
                  <th className="px-4 py-3 text-right font-medium">ARO P90</th>
                  <th className="px-4 py-3 font-medium">Confidence</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {topRiskAssets.map((asset) => (
                  <tr key={asset.api} className="border-t border-[#2e2e36] hover:bg-[#2a2a32] transition-colors">
                    <td className="px-4 py-3 font-mono text-sm text-indigo-400">{asset.api}</td>
                    <td className="px-4 py-3 text-sm text-zinc-200">{asset.name}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{asset.basin}</td>
                    <td className="px-4 py-3 font-mono text-sm text-right text-zinc-200">{asset.p50}</td>
                    <td className="px-4 py-3 font-mono text-sm text-right text-zinc-400">{asset.p90}</td>
                    <td className="px-4 py-3"><ConfidenceBadge level={asset.confidence} /></td>
                    <td className="px-4 py-3"><StatusBadge status={asset.status} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </main>
    </div>
  );
}
