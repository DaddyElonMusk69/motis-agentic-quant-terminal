import { useMemo } from "react";
import { useAppRouter } from "../app/router";
import { DashboardPage } from "../pages/DashboardPage";
import { DataPage } from "../pages/DataPage";
import { EnginesPage } from "../pages/EnginesPage";
import { ResearchDevelopmentPage } from "../pages/ResearchDevelopmentPage";
import { ResearchSignalDiscoveryPage } from "../pages/ResearchSignalDiscoveryPage";
import { ResearchStage0Page } from "../pages/ResearchStage0Page";
import { TradingPage } from "../pages/TradingPage";
import { SidebarNav } from "./SidebarNav";

export function TerminalShell() {
  const { route, navigate } = useAppRouter();
  const page = useMemo(() => {
    switch (route) {
      case "/data":
        return <DataPage />;
      case "/engines":
        return <EnginesPage />;
      case "/research/stage0":
        return <ResearchStage0Page />;
      case "/research/discovery":
        return <ResearchSignalDiscoveryPage />;
      case "/research/development":
        return <ResearchDevelopmentPage />;
      case "/trading":
        return <TradingPage />;
      case "/dashboard":
      default:
        return <DashboardPage />;
    }
  }, [route]);

  return (
    <div className="terminal-app">
      <SidebarNav navigate={navigate} route={route} />
      <main className="terminal-workspace">{page}</main>
    </div>
  );
}
