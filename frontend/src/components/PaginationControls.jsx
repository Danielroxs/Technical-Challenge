export default function PaginationControls({
  currentPage,
  totalPages,
  onPrevious,
  onNext,
}) {
  return (
    // Simple client-side pagination controls driven by backend pagination metadata
    <div style={{ marginTop: "16px", textAlign: "center" }}>
      <button onClick={onPrevious} disabled={currentPage === 1}>
        Previous
      </button>

      <span style={{ margin: "0 12px" }}>
        Page {currentPage} of {totalPages}
      </span>

      <button onClick={onNext} disabled={currentPage === totalPages}>
        Next
      </button>
    </div>
  );
}
