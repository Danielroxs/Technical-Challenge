export default function RefreshButton({ refreshing, onRefresh }) {
  return (
    <button onClick={onRefresh} disabled={refreshing}>
      {refreshing && <span className="button-spinner" aria-hidden="true" />}
      <span>{refreshing ? "Refreshing..." : "Refresh data"}</span>
    </button>
  );
}
