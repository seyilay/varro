import Sidebar from "@/components/Sidebar";

export default function Settings() {
  return (
    <div className="flex h-screen bg-[#1a1a1f]">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        <header className="h-14 bg-[#222228] border-b border-[#3a3a44] flex items-center px-6">
          <span className="text-zinc-200 text-sm">Settings</span>
        </header>
        <div className="p-6">
          <div className="max-w-2xl">
            <h2 className="text-lg font-medium text-zinc-200 mb-6">Account Settings</h2>
            <div className="space-y-6">
              <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
                <h3 className="text-sm font-medium text-zinc-200 mb-3">Organization</h3>
                <div className="space-y-3">
                  <div>
                    <label className="text-xs text-zinc-500 uppercase tracking-wider font-medium">Company Name</label>
                    <input type="text" defaultValue="Murphy Oil Corporation" className="mt-1 w-full h-9 bg-[#2a2a32] border border-[#3a3a44] rounded-lg px-3 text-sm text-zinc-200 focus:outline-none focus:border-indigo-500" />
                  </div>
                  <div>
                    <label className="text-xs text-zinc-500 uppercase tracking-wider font-medium">Plan</label>
                    <div className="mt-1 text-sm text-zinc-200">Professional <span className="text-indigo-400 ml-2 font-medium">Upgrade →</span></div>
                  </div>
                </div>
              </div>
              <div className="bg-[#222228] border border-[#3a3a44] rounded-lg p-4">
                <h3 className="text-sm font-medium text-zinc-200 mb-3">Data Sharing</h3>
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-sm text-zinc-200">Contribute anonymized cost data</div>
                    <div className="text-xs text-zinc-500">Help improve estimates for all users</div>
                  </div>
                  <button className="w-12 h-6 bg-indigo-500 rounded-full relative">
                    <div className="absolute right-1 top-1 w-4 h-4 bg-white rounded-full" />
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
