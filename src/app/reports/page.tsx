import Sidebar from "@/components/Sidebar";
import { FileText, Download, Clock } from "lucide-react";

const recentReports = [
  { name: "FASB ASC 410 Disclosure Memo — Q1 2026", date: "Mar 28, 2026", type: "PDF", size: "2.4 MB" },
  { name: "Portfolio ARO Summary — Full Year 2025", date: "Mar 15, 2026", type: "Excel", size: "1.8 MB" },
  { name: "Regulatory Compliance Report — Mar 2026", date: "Mar 01, 2026", type: "PDF", size: "890 KB" },
  { name: "FASB ASC 410 Disclosure Memo — Q4 2025", date: "Jan 15, 2026", type: "PDF", size: "2.1 MB" },
];

export default function Reports() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center px-6">
          <span className="text-zinc-200 text-sm">Reports</span>
        </header>

        <div className="p-6">
          {/* Generate New Report */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-6 mb-6">
            <h2 className="text-sm font-medium text-zinc-200 mb-4">Generate New Report</h2>
            <div className="grid grid-cols-3 gap-4">
              <button className="flex flex-col items-center gap-3 p-6 border border-[#3a3a44] rounded-lg hover:border-indigo-500/50 hover:bg-indigo-500/5 transition-colors">
                <FileText className="w-8 h-8 text-indigo-400" />
                <span className="text-sm text-zinc-200">FASB ASC 410 Memo</span>
                <span className="text-xs text-zinc-500">Audit-ready disclosure</span>
              </button>
              <button className="flex flex-col items-center gap-3 p-6 border border-[#3a3a44] rounded-lg hover:border-indigo-500/50 hover:bg-indigo-500/5 transition-colors">
                <FileText className="w-8 h-8 text-indigo-400" />
                <span className="text-sm text-zinc-200">Portfolio Summary</span>
                <span className="text-xs text-zinc-500">Full ARO breakdown</span>
              </button>
              <button className="flex flex-col items-center gap-3 p-6 border border-[#3a3a44] rounded-lg hover:border-indigo-500/50 hover:bg-indigo-500/5 transition-colors">
                <FileText className="w-8 h-8 text-indigo-400" />
                <span className="text-sm text-zinc-200">Compliance Report</span>
                <span className="text-xs text-zinc-500">Regulatory status</span>
              </button>
            </div>
          </div>

          {/* Recent Reports */}
          <div className="bg-[#222228] border border-[#3a3a44] rounded-lg">
            <div className="px-4 py-3 border-b border-[#3a3a44]">
              <span className="text-xs text-zinc-500 uppercase tracking-wider font-medium">RECENT REPORTS</span>
            </div>
            <div className="divide-y divide-[#3a3a44]">
              {recentReports.map((report, i) => (
                <div key={i} className="flex items-center justify-between px-4 py-3 hover:bg-[#2a2a32] transition-colors">
                  <div className="flex items-center gap-3">
                    <FileText className="w-5 h-5 text-zinc-500" />
                    <div>
                      <div className="text-sm text-zinc-200">{report.name}</div>
                      <div className="flex items-center gap-2 text-xs text-zinc-500">
                        <Clock className="w-3 h-3" />
                        {report.date}
                        <span>·</span>
                        <span>{report.type}</span>
                        <span>·</span>
                        <span>{report.size}</span>
                      </div>
                    </div>
                  </div>
                  <button className="flex items-center gap-2 px-3 py-1.5 text-sm text-indigo-400 hover:text-indigo-300 font-medium">
                    <Download className="w-4 h-4" />
                    Download
                  </button>
                </div>
              ))}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
