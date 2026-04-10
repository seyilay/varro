import Sidebar from "@/components/Sidebar";
import { AlertTriangle, Download, Plus, FileText } from "lucide-react";

const wellAttributes = [
  { label: "API NUMBER", value: "177-054-12345-0000" },
  { label: "WELL TYPE", value: "OFFSHORE / FIXED PLATFORM" },
  { label: "WATER DEPTH", value: "312 ft" },
  { label: "TOTAL DEPTH", value: "14,200 ft MD" },
  { label: "VINTAGE", value: "1987" },
  { label: "COMPLETION TYPE", value: "OPEN HOLE" },
  { label: "OPERATOR", value: "Murphy Oil Corp." },
  { label: "REGULATORY JURIS.", value: "US GOM / BOEM Region 3" },
];

const comparableWells = [
  { api: "177-054-98765-0001", basin: "GOM", waterDepth: "287 ft", totalDepth: "13,400 ft", vintage: "1984", actualCost: "$1,940,000", yearPad: "2019", source: "BOEM" },
  { api: "177-054-87654-0002", basin: "GOM", waterDepth: "340 ft", totalDepth: "15,100 ft", vintage: "1989", actualCost: "$2,280,000", yearPad: "2020", source: "BOEM" },
  { api: "177-054-76543-0003", basin: "GOM", waterDepth: "298 ft", totalDepth: "12,800 ft", vintage: "1982", actualCost: "$1,720,000", yearPad: "2018", source: "BSEE" },
  { api: "177-054-65432-0004", basin: "GOM", waterDepth: "325 ft", totalDepth: "14,900 ft", vintage: "1990", actualCost: "$2,450,000", yearPad: "2021", source: "BOEM" },
  { api: "177-054-54321-0005", basin: "GOM", waterDepth: "305 ft", totalDepth: "13,200 ft", vintage: "1985", actualCost: "$2,010,000", yearPad: "2019", source: "BSEE" },
];

export default function AROEstimator() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      
      <main className="flex-1 overflow-auto">
        {/* Top Bar */}
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center justify-between px-6">
          <div className="flex items-center gap-2">
            <span className="text-zinc-400 text-sm">ARO Estimator</span>
            <span className="text-zinc-600">/</span>
            <span className="text-indigo-400 font-mono text-sm">177-054-12345-0000</span>
          </div>
        </header>

        {/* Content */}
        <div className="p-6">
          {/* Well Header */}
          <div className="mb-6">
            <div className="flex items-start justify-between">
              <div>
                <h1 className="text-xl font-mono text-indigo-400 mb-1">API: 177-054-12345-0000</h1>
                <p className="text-zinc-400">Murphy Oil Corp. — Well 42-A — US GOM / BOEM Region 3</p>
              </div>
              <div className="flex items-center gap-2 bg-rose-500/10 border border-rose-500/20 px-3 py-1.5 rounded-lg">
                <AlertTriangle className="w-4 h-4 text-rose-400" />
                <span className="text-sm text-rose-400 font-medium">DELINQUENT — BOEM LIST SINCE 2021</span>
              </div>
            </div>
          </div>

          {/* Well Attributes */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-4 mb-6">
            <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">WELL ATTRIBUTES</div>
            <div className="grid grid-cols-4 gap-4">
              {wellAttributes.map((attr) => (
                <div key={attr.label}>
                  <div className="text-xs text-zinc-500 uppercase tracking-wider mb-1 font-medium">{attr.label}</div>
                  <div className="font-mono text-sm text-zinc-200">{attr.value}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Estimate Output */}
          <div className="grid grid-cols-2 gap-6 mb-6">
            {/* Cost Estimate */}
            <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-6">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">P50 ESTIMATE</div>
              <div className="text-4xl font-semibold text-indigo-400 mb-4">$2,140,000</div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">P90 (CONSERVATIVE)</span>
                  <span className="font-mono text-zinc-300">$3,820,000</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">P10 (OPTIMISTIC)</span>
                  <span className="font-mono text-emerald-400">$1,290,000</span>
                </div>
              </div>
            </div>

            {/* Confidence Band */}
            <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-6">
              <div className="text-xs text-zinc-500 uppercase tracking-wider mb-4 font-medium">CONFIDENCE BAND</div>
              
              {/* Range Visualization */}
              <div className="mb-6">
                <div className="relative h-3 bg-[#2a2a32] rounded-full mb-2">
                  <div className="absolute left-[15%] right-[20%] h-full bg-indigo-500/30 rounded-full" />
                  <div className="absolute left-[45%] w-1 h-5 -top-1 bg-indigo-500 rounded" />
                </div>
                <div className="flex justify-between text-xs font-mono text-zinc-500">
                  <span>$1.3M (P10)</span>
                  <span className="text-indigo-400">$2.1M (P50)</span>
                  <span>$3.8M (P90)</span>
                </div>
              </div>

              <div className="space-y-3">
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">CONFIDENCE LEVEL</span>
                  <span className="font-medium text-amber-400">MEDIUM</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">DATA DENSITY</span>
                  <span className="font-mono text-zinc-200">42 COMPARABLES</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">ESTIMATE DATE</span>
                  <span className="font-mono text-zinc-200">2026-03-30</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-zinc-500">MODEL VERSION</span>
                  <span className="font-mono text-zinc-200">v0.4.1</span>
                </div>
              </div>
            </div>
          </div>

          {/* Comparable Wells Table */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg mb-6">
            <div className="px-4 py-3 border-b border-[#3a3a44] flex items-center justify-between">
              <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">COMPARABLE WELLS (42)</span>
              <button className="text-xs text-indigo-400 hover:text-indigo-300 font-medium">Expand All</button>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="text-left text-xs text-zinc-500 uppercase tracking-wider">
                    <th className="px-4 py-3 font-medium">API #</th>
                    <th className="px-4 py-3 font-medium">Basin</th>
                    <th className="px-4 py-3 font-medium">Water Depth</th>
                    <th className="px-4 py-3 font-medium">Total Depth</th>
                    <th className="px-4 py-3 font-medium">Vintage</th>
                    <th className="px-4 py-3 text-right font-medium">Actual P&A Cost</th>
                    <th className="px-4 py-3 font-medium">Year P&A'd</th>
                    <th className="px-4 py-3 font-medium">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {comparableWells.map((well) => (
                    <tr key={well.api} className="border-t border-[#2e2e36] hover:bg-[#2a2a32] transition-colors">
                      <td className="px-4 py-3 font-mono text-sm text-indigo-400">{well.api}</td>
                      <td className="px-4 py-3 text-sm text-zinc-400">{well.basin}</td>
                      <td className="px-4 py-3 font-mono text-sm text-zinc-200">{well.waterDepth}</td>
                      <td className="px-4 py-3 font-mono text-sm text-zinc-200">{well.totalDepth}</td>
                      <td className="px-4 py-3 font-mono text-sm text-zinc-200">{well.vintage}</td>
                      <td className="px-4 py-3 font-mono text-sm text-right text-indigo-300">{well.actualCost}</td>
                      <td className="px-4 py-3 font-mono text-sm text-zinc-200">{well.yearPad}</td>
                      <td className="px-4 py-3">
                        <span className="text-xs font-medium text-indigo-400 bg-indigo-500/10 px-1.5 py-0.5 rounded">{well.source}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Export Actions */}
          <div className="flex items-center gap-4">
            <button className="flex items-center gap-2 px-6 py-3 bg-indigo-500 text-white font-medium rounded-lg hover:bg-indigo-600 transition-colors">
              <FileText className="w-5 h-5" />
              Export FASB ASC 410 Memo (PDF)
            </button>
            <button className="flex items-center gap-2 px-6 py-3 border border-[#3a3a44] text-zinc-200 rounded-lg hover:bg-[#2a2a32] transition-colors">
              <Download className="w-5 h-5" />
              Export to Excel
            </button>
            <button className="flex items-center gap-2 px-4 py-3 text-indigo-400 hover:text-indigo-300 transition-colors font-medium">
              <Plus className="w-5 h-5" />
              Add to Portfolio
            </button>
          </div>
        </div>
      </main>
    </div>
  );
}
