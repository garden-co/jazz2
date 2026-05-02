export function readCanvasIdFromUrl() {
  const canvasId = new URLSearchParams(window.location.search).get("canvas");
  return canvasId && canvasId.length > 0 ? canvasId : null;
}

export function writeCanvasIdToUrl(canvasId: string) {
  const nextUrl = new URL(window.location.href);
  nextUrl.searchParams.set("canvas", canvasId);
  window.history.replaceState(
    window.history.state,
    "",
    `${nextUrl.pathname}${nextUrl.search}${nextUrl.hash}`,
  );
}
