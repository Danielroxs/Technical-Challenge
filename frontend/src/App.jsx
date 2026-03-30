import { useCallback, useEffect, useState } from "react";
import { getOutages, refreshOutages } from "./services/api";
import OutagesTable from "./components/OutagesTable";
import SearchForm from "./components/SearchForm";
import PaginationControls from "./components/PaginationControls";
import SortControls from "./components/SortControls";
import DateFilters from "./components/DateFilters";
import RefreshButton from "./components/RefreshButton";
import StatusCard from "./components/StatusCard";
import "./App.css";
import RefreshMessage from "./components/RefreshMessage";

function App() {
  // Main dataset returned by the backend
  const [items, setItems] = useState([]);

  // Pagination metadata from /data endpoint
  const [pagination, setPagination] = useState(null);

  // Global fetch state for table data
  const [loading, setLoading] = useState(true);

  // User-friendly error for /data requests
  const [error, setError] = useState("");

  // Separate input value from applied search value
  // so the table only reloads when the form is submitted
  const [inputPlantName, setInputPlantName] = useState("");
  const [searchPlantName, setSearchPlantName] = useState("");

  // Current page sent to the API
  const [currentPage, setCurrentPage] = useState(1);

  // Sorting controls mapped to backend query params
  const [sortBy, setSortBy] = useState("period");
  const [sortOrder, setSortOrder] = useState("desc");

  // Date range filters
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  // Independent state for refresh action
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState("");
  const [refreshSuccess, setRefreshSuccess] = useState("");

  // Prevent selecting future dates in filters
  const today = new Date().toISOString().split("T")[0];

  // Used to enable/disable "Clear filters" button
  const hasActiveFilters = Boolean(
    inputPlantName || searchPlantName || startDate || endDate,
  );

  // Centralized table loader:
  // calls /data with current filters, pagination and sorting
  const loadOutages = useCallback(async () => {
    try {
      setLoading(true);
      setError("");

      const data = await getOutages({
        page: currentPage,
        limit: 20,
        plantName: searchPlantName || undefined,
        startDate: startDate || undefined,
        endDate: endDate || undefined,
        sortBy,
        sortOrder,
      });

      setItems(data.items || []);
      setPagination(data.pagination || null);
    } catch (err) {
      setError(err.message || "Failed to load outage data.");
    } finally {
      setLoading(false);
    }
  }, [searchPlantName, currentPage, sortBy, sortOrder, startDate, endDate]);

  // Reload data whenever applied filters or pagination change
  useEffect(() => {
    loadOutages();
  }, [loadOutages]);

  // Apply plant name filter only on form submit
  function handleSubmit(event) {
    event.preventDefault();
    setCurrentPage(1);
    setSearchPlantName(inputPlantName.trim());
  }

  // Trigger backend refresh and then reload current table view
  async function handleRefresh() {
    try {
      setRefreshing(true);
      setRefreshError("");
      setRefreshSuccess("");

      await refreshOutages();
      await loadOutages();

      setRefreshSuccess("Data refreshed successfully.");
    } catch (err) {
      setRefreshError(err.message || "Failed to refresh outage data.");
    } finally {
      setRefreshing(false);
    }
  }

  // Basic pagination controls
  function handlePreviousPage() {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  }

  function handleNextPage() {
    if (pagination && currentPage < pagination.total_pages) {
      setCurrentPage(currentPage + 1);
    }
  }

  // Validate start date:
  // - no future dates
  // - keep end date aligned if it becomes invalid
  function handleStartDateChange(value) {
    if (value && value > today) {
      return;
    }

    setCurrentPage(1);
    setStartDate(value);

    if (endDate && value && endDate < value) {
      setEndDate(value);
    }
  }

  // Validate end date:
  // - no future dates
  // - cannot be earlier than start date
  function handleEndDateChange(value) {
    if (value && value > today) {
      return;
    }

    if (startDate && value && value < startDate) {
      return;
    }

    setCurrentPage(1);
    setEndDate(value);
  }

  // Reset all filters and return to page 1
  function handleClearFilters() {
    setInputPlantName("");
    setSearchPlantName("");
    setStartDate("");
    setEndDate("");
    setCurrentPage(1);
  }

  return (
    <main>
      <h1>Nuclear Outages Dashboard</h1>

      <section className="controls-panel">
        <SearchForm
          value={inputPlantName}
          onChange={(event) => setInputPlantName(event.target.value)}
          onSubmit={handleSubmit}
        />

        <div className="actions-row">
          <button
            type="button"
            onClick={handleClearFilters}
            disabled={!hasActiveFilters}
          >
            Clear filters
          </button>

          <RefreshButton refreshing={refreshing} onRefresh={handleRefresh} />
        </div>

        <DateFilters
          startDate={startDate}
          endDate={endDate}
          maxDate={today}
          onStartDateChange={handleStartDateChange}
          onEndDateChange={handleEndDateChange}
        />

        <SortControls
          sortBy={sortBy}
          sortOrder={sortOrder}
          onSortByChange={(event) => {
            // Reset to first page when sorting changes
            setCurrentPage(1);
            setSortBy(event.target.value);
          }}
          onSortOrderChange={(event) => {
            // Reset to first page when sorting changes
            setCurrentPage(1);
            setSortOrder(event.target.value);
          }}
        />
      </section>

      {/* Feedback messages for refresh action */}
      {refreshError && (
        <RefreshMessage
          key={`error-${refreshError}`}
          message={refreshError}
          variant="error"
        />
      )}

      {refreshSuccess && (
        <RefreshMessage
          key={`success-${refreshSuccess}`}
          message={refreshSuccess}
          variant="success"
        />
      )}

      <section className="results-area">
        {/* Loading state */}
        {loading && (
          <StatusCard
            title="Loading outage data"
            message="Fetching records from the API and preparing the table."
            showSpinner
          />
        )}

        {/* Error state */}
        {!loading && error && (
          <StatusCard
            title="Something went wrong"
            message={error}
            variant="error"
          />
        )}

        {/* Empty state */}
        {!loading && !error && items.length === 0 && (
          <StatusCard
            title="No outage records found"
            message="Try changing the plant name or date filters."
            variant="empty"
          />
        )}

        {/* Success state */}
        {!loading && !error && items.length > 0 && (
          <>
            <p className="summary-text">
              Rows loaded: {items.length}
              {pagination &&
                ` | Total rows: ${pagination.total} | Page ${pagination.page} of ${pagination.total_pages}`}
            </p>

            <OutagesTable items={items} />

            {pagination && pagination.total_pages > 1 && (
              <PaginationControls
                currentPage={currentPage}
                totalPages={pagination.total_pages}
                onPrevious={handlePreviousPage}
                onNext={handleNextPage}
              />
            )}
          </>
        )}
      </section>
    </main>
  );
}

export default App;
