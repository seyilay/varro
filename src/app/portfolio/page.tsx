import Sidebar from "@/components/Sidebar";
import { Search, Upload, Download } from "lucide-react";

const summaryStats = [
  { label: "TOTAL P50", value: "$142.3M", color: "text-indigo-400" },
  { label: "TOTAL P90", value: "$198.7M", color: "text-zinc-100" },
  { label: "ASSETS", value: "247", color: "text-zinc-100" },
  { label: "HIGH UNCERTAINTY", value: "31", color: "text-amber-400" },
  { label: "DELINQUENT", value: "12", color: "text-rose-400" },
];

const assets = [
  { api: "177-054-12345-0000", name: "Well A-42", basin: "GOM", p50: "$3.1M", p90: "$5.2M", confidence: "HIGH", status: "PRODUCING", delta: "+$12K" },
  { api: "177-054-67890-0001", name: "Platform B-7", basin: "GOM", p50: "$2.4M", p90: "$3.9M", confidence: "MEDIUM", status: "IDLE", delta: "-$8K" },
  { api: "177-054-11111-0002", name: "Well C-15", basin: "GOM", p50: "$2.1M", p90: "$3.4M", confidence: "HIGH", status: "PRODUCING", delta: "+$45K" },
  { api: "177-054-22222-0003", name: "Well D-23", basin: "GOM", p50: "$1.8M", p90: "$2.9M", confidence: "LOW", status: "DELINQUENT", delta: "+$180K" },
  { api: "177-054-33333-0004", name: "Platform E-1", basin: "GOM", p50: "$1.2M", p90: "$2.1M", confidence: "MEDIUM", status: "IDLE", delta: "-$22K" },
  { api: "177-054-44444-0005", name: "Well F-8", basin: "GOM", p50: "$980K", p90: "$1.6M", confidence: "HIGH", status: "PRODUCING", delta: "+$5K" },
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

function DeltaBadge({ delta }: { delta: string }) {
  const isPositive = delta.startsWith('+');
  return (
    <span className={`text-xs font-medium ${isPositive ? 'text-rose-400' : 'text-emerald-400'}`}>
      {delta}
    </span>
  );
}

export default function Portfolio() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      
      <main className="flex-1 overflow-auto">
        {/* Top Bar */}
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center justify-between px-6">
          <div className="flex items-center gap-2 text-sm text-zinc-400">
            <span className="text-zinc-200">Portfolio View</span>
          </div>
          <div className="flex items-center gap-3">
            <button className="flex items-center gap-2 px-4 py-2 bg-indigo-500 text-white text-sm font-medium rounded-lg hover:bg-indigo-600 transition-colors">
              <Upload className="w-4 h-4" />
              Upload CSV
            </button>
            <button className="flex items-center gap-2 px-4 py-2 border border-[#3a3a44] text-zinc-200 text-sm rounded-lg hover:bg-[#2a2a32] transition-colors">
              <Download className="w-4 h-4" />
              Export Report
            </button>
          </div>
        </header>

        {/* Content */}
        <div className="p-6">
          {/* Summary Strip */}
          <div className="flex gap-4 mb-6 overflow-x-auto pb-2">
            {summaryStats.map((stat) => (
              <div key={stat.label} className="flex items-center gap-3 bg-[#222228] border border-[#3a3a44] rounded-lg px-4 py-2 whitespace-nowrap">
                <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">{stat.label}</span>
                <span className={`font-semibold ${stat.color}`}>{stat.value}</span>
              </div>
            ))}
          </div>

          {/* Charts Grid */}
          <div className="grid grid-cols-5 gap-6 mb-6">
            {/* Waterfall Chart */}
            <div className="col-span-3 bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">ARO EXPOSURE BY ASSET (P50)</div>
              <div className="space-y-3">
                {assets.slice(0, 6).map((asset) => (
                  <div key={asset.api} className="flex items-center gap-4">
                    <span className="text-sm text-zinc-400 w-24 truncate">{asset.name}</span>
                    <div className="flex-1 h-5 bg-[#2a2a32] rounded overflow-hidden">
                      <div 
                        className={`h-full rounded ${
                          asset.confidence === 'HIGH' ? 'bg-indigo-500' :
                          asset.confidence === 'MEDIUM' ? 'bg-amber-500' : 'bg-rose-500'
                        }`}
                        style={{ width: `${(parseFloat(asset.p50.replace(/[$MK]/g, '')) / 3.5) * 100}%` }}
                      />
                    </div>
                    <span className="font-mono text-sm text-indigo-400 w-16 text-right">{asset.p50}</span>
                  </div>
                ))}
              </div>
              <a href="#" className="text-xs text-indigo-400 hover:text-indigo-300 mt-4 block font-medium">View All (247) →</a>
            </div>

            {/* Confidence Donut */}
            <div className="col-span-2 bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">BY CONFIDENCE</div>
              <div className="flex items-center justify-center h-40">
                <div className="relative w-32 h-32">
                  <div className="absolute inset-0 rounded-full border-8 border-indigo-500/30" />
                  <div className="absolute inset-0 flex items-center justify-center">
                    <span className="text-xl font-semibold text-zinc-100">247</span>
                  </div>
                </div>
              </div>
              <div className="flex justify-center gap-6 mt-4">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-indigo-500 rounded-full" />
                  <span className="text-xs text-zinc-400">High 42%</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-amber-500 rounded-full" />
                  <span className="text-xs text-zinc-400">Medium 38%</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-rose-500 rounded-full" />
                  <span className="text-xs text-zinc-400">Low 20%</span>
                </div>
              </div>
            </div>
          </div>

          {/* Full Asset Table */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg">
            <div className="px-4 py-3 border-b border-[#3a3a44] flex items-center justify-between">
              <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">ALL ASSETS (247)</span>
              <div className="flex items-center gap-3">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                  <input 
                    type="text" 
                    placeholder="Search assets..." 
                    className="bg-[#2a2a32] border border-[#3a3a44] rounded-lg pl-9 pr-4 py-1.5 text-sm text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>
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
                  <th className="px-4 py-3 text-right font-medium">∆ QoQ</th>
                </tr>
              </thead>
              <tbody>
                {assets.map((asset) => (
                  <tr key={asset.api} className="border-t border-[#2e2e36] hover:bg-[#2a2a32] transition-colors cursor-pointer">
                    <td className="px-4 py-3 font-mono text-sm text-indigo-400">{asset.api}</td>
                    <td className="px-4 py-3 text-sm text-zinc-200">{asset.name}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{asset.basin}</td>
                    <td className="px-4 py-3 font-mono text-sm text-right text-zinc-200">{asset.p50}</td>
                    <td className="px-4 py-3 font-mono text-sm text-right text-zinc-400">{asset.p90}</td>
                    <td className="px-4 py-3"><ConfidenceBadge level={asset.confidence} /></td>
                    <td className="px-4 py-3"><StatusBadge status={asset.status} /></td>
                    <td className="px-4 py-3 text-right"><DeltaBadge delta={asset.delta} /></td>
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
