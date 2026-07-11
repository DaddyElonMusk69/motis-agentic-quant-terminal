import { BarChart3, ChevronDown, Database, FlaskConical, Gauge, RadioTower, TrendingUp } from "lucide-react";
import type { AppRoute } from "../app/router";

type SidebarNavProps = {
  route: AppRoute;
  navigate: (route: AppRoute) => void;
};

const primaryItems: Array<{
  label: string;
  route: AppRoute;
  icon: typeof Gauge;
}> = [
  { label: "Dashboard", route: "/dashboard", icon: Gauge },
  { label: "Data", route: "/data", icon: Database },
  { label: "Engines", route: "/engines", icon: RadioTower },
  { label: "R&D", route: "/research/stage0", icon: FlaskConical },
  { label: "Trading", route: "/trading", icon: TrendingUp }
];

const researchItems: Array<{ label: string; route: AppRoute }> = [
  { label: "Signal Discovery", route: "/research/discovery" },
  { label: "Training Pools", route: "/research/stage0" },
  { label: "Development", route: "/research/development" }
];

export function SidebarNav({ route, navigate }: SidebarNavProps) {
  const inResearch = route.startsWith("/research");

  return (
    <aside className="terminal-sidebar">
      <div className="brand-lockup">
        <BarChart3 aria-hidden="true" />
        <div>
          <strong>Motis</strong>
          <span>Quant Terminal</span>
        </div>
      </div>

      <nav className="primary-nav" aria-label="Primary navigation">
        {primaryItems.map((item) => {
          const Icon = item.icon;
          const isActive = item.route === route || (item.route === "/research/stage0" && inResearch);
          if (item.label === "R&D") {
            return (
              <div className={inResearch ? "nav-group is-open" : "nav-group"} key={item.label}>
                <button className={isActive ? "nav-item is-active" : "nav-item"} onClick={() => navigate(item.route)} type="button">
                  <Icon aria-hidden="true" />
                  <span>{item.label}</span>
                  <ChevronDown className="nav-item__chevron" aria-hidden="true" />
                </button>
                {inResearch ? (
                  <div className="nav-children">
                    {researchItems.map((child) => (
                      <button className={route === child.route ? "subnav-item is-active" : "subnav-item"} key={child.route} onClick={() => navigate(child.route)} type="button">
                        {child.label}
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            );
          }
          return (
            <button className={isActive ? "nav-item is-active" : "nav-item"} key={item.label} onClick={() => navigate(item.route)} type="button">
              <Icon aria-hidden="true" />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>

      <div className="system-tile">
        <span className="system-dot" />
        <div>
          <strong>System Status</strong>
          <span>v2 shell online</span>
        </div>
      </div>

      <div className="sidebar-foot">
        <span>motis.local</span>
        <span>v2.0 foundation</span>
      </div>
    </aside>
  );
}
