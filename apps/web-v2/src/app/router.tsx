import { useCallback, useEffect, useMemo, useSyncExternalStore } from "react";

export type AppRoute =
  | "/dashboard"
  | "/data"
  | "/engines"
  | "/research/discovery"
  | "/research/stage0"
  | "/research/development"
  | "/trading";

const routes: AppRoute[] = [
  "/dashboard",
  "/data",
  "/engines",
  "/research/discovery",
  "/research/stage0",
  "/research/development",
  "/trading"
];

function normalizePath(pathname: string): AppRoute {
  const cleanPath = pathname.replace(/\/+$/, "") || "/dashboard";
  if (routes.includes(cleanPath as AppRoute)) {
    return cleanPath as AppRoute;
  }
  return "/dashboard";
}

function subscribe(listener: () => void) {
  window.addEventListener("popstate", listener);
  return () => window.removeEventListener("popstate", listener);
}

function getSnapshot() {
  return `${window.location.pathname}${window.location.search}`;
}

export function useAppRouter() {
  const locationKey = useSyncExternalStore(subscribe, getSnapshot, () => "/dashboard");
  const url = useMemo(() => new URL(locationKey, window.location.origin), [locationKey]);
  const route = normalizePath(url.pathname);

  useEffect(() => {
    if (url.pathname !== route) {
      window.history.replaceState(null, "", route);
    }
  }, [route, url.pathname]);

  const navigate = useCallback((nextRoute: AppRoute, search = "") => {
    const nextUrl = `${nextRoute}${search}`;
    if (`${window.location.pathname}${window.location.search}` === nextUrl) {
      return;
    }
    window.history.pushState(null, "", nextUrl);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }, []);

  return {
    route,
    searchParams: url.searchParams,
    navigate
  };
}
