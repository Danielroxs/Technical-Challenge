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
  const [items, setItems] = useState([]);
  const [pagination, setPagination] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [inputPlantName, setInputPlantName] = useState("");
  const [searchPlantName, setSearchPlantName] = useState("");
  const [currentPage, setCurrentPage] = useState(1);

  const [sortBy, setSortBy] = useState("period");
  const [sortOrder, setSortOrder] = useState("desc");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState("");
  const [refreshSuccess, setRefreshSuccess] = useState("");

  const today = new Date().toISOString().split("T")[0];

  const hasActiveFilters = Boolean(
    inputPlantName || searchPlantName || startDate || endDate,
  );

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

  useEffect(() => {
    loadOutages();
  }, [loadOutages]);

  function handleSubmit(event) {
    event.preventDefault();
    setCurrentPage(1);
    setSearchPlantName(inputPlantName.trim());
  }

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
            setCurrentPage(1);
            setSortBy(event.target.value);
          }}
          onSortOrderChange={(event) => {
            setCurrentPage(1);
            setSortOrder(event.target.value);
          }}
        />
      </section>

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
        {loading && (
          <StatusCard
            title="Loading outage data"
            message="Fetching records from the API and preparing the table."
            showSpinner
          />
        )}

        {!loading && error && (
          <StatusCard
            title="Something went wrong"
            message={error}
            variant="error"
          />
        )}

        {!loading && !error && items.length === 0 && (
          <StatusCard
            title="No outage records found"
            message="Try changing the plant name or date filters."
            variant="empty"
          />
        )}

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
