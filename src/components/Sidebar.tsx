"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { 
  Home, 
  Search, 
  Calculator, 
  Briefcase, 
  FileText, 
  Bell,
  Settings,
  ChevronLeft
} from "lucide-react";
import { useState } from "react";

const navItems = [
  { name: "Dashboard", href: "/", icon: Home },
  { name: "Search", href: "/search", icon: Search },
  { name: "ARO Estimator", href: "/estimate", icon: Calculator },
  { name: "Portfolio", href: "/portfolio", icon: Briefcase },
  { name: "Regulatory", href: "/regulatory", icon: Bell },
  { name: "Reports", href: "/reports", icon: FileText },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside className={`${collapsed ? 'w-16' : 'w-60'} h-screen bg-[#222228] border-r border-[#3a3a44] flex flex-col transition-all duration-200`}>
      {/* Logo */}
      <div className="h-14 flex items-center px-4 border-b border-[#3a3a44]">
        <span className={`font-semibold text-zinc-100 ${collapsed ? 'text-lg' : 'text-xl'}`}>
          {collapsed ? 'V' : 'Varro'}
        </span>
        <button 
          onClick={() => setCollapsed(!collapsed)}
          className="ml-auto text-zinc-500 hover:text-zinc-300"
        >
          <ChevronLeft className={`w-4 h-4 transition-transform ${collapsed ? 'rotate-180' : ''}`} />
        </button>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4">
        {navItems.map((item) => {
          const isActive = pathname === item.href;
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center gap-3 px-4 py-3 text-sm transition-colors ${
                isActive
                  ? 'bg-indigo-500/10 text-indigo-400 border-l-2 border-indigo-500'
                  : 'text-zinc-400 hover:text-zinc-200 hover:bg-[#2a2a32] border-l-2 border-transparent'
              }`}
            >
              <Icon className="w-5 h-5 flex-shrink-0" />
              {!collapsed && <span>{item.name}</span>}
            </Link>
          );
        })}
      </nav>

      {/* Settings */}
      <div className="border-t border-[#3a3a44] py-4">
        <Link
          href="/settings"
          className="flex items-center gap-3 px-4 py-3 text-sm text-zinc-400 hover:text-zinc-200 hover:bg-[#2a2a32]"
        >
          <Settings className="w-5 h-5 flex-shrink-0" />
          {!collapsed && <span>Settings</span>}
        </Link>
      </div>
    </aside>
  );
}
