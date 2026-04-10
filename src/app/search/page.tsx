import Sidebar from "@/components/Sidebar";
import { Search as SearchIcon } from "lucide-react";

export default function SearchPage() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center px-6">
          <span className="text-zinc-200 text-sm">Asset Search</span>
        </header>
        <div className="p-6">
          <div className="max-w-4xl mx-auto">
            <div className="relative mb-8">
              <SearchIcon className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-500" />
              <input 
                type="text" 
                placeholder="Enter API number, operator name, or lease ID..."
                className="w-full h-12 bg-[#222228] border border-[#3a3a44] rounded-lg pl-12 pr-4 font-mono text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-indigo-500"
              />
            </div>
            <div className="text-center text-zinc-500 py-20">
              <SearchIcon className="w-12 h-12 mx-auto mb-4 opacity-30" />
              <p>Search for wells by API number, operator, or lease ID</p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
