export default function DateFilters({
  startDate,
  endDate,
  maxDate,
  onStartDateChange,
  onEndDateChange,
}) {
  return (
    <div className="date-filters">
      <label className="field-group">
        <span>Start date</span>
        <input
          type="date"
          value={startDate}
          // Prevent selecting a start date after the current end date or after today
          max={endDate || maxDate}
          onChange={(event) => onStartDateChange(event.target.value)}
        />
      </label>

      <label className="field-group">
        <span>End date</span>
        <input
          type="date"
          value={endDate}
          // Keep the end date within a valid range: not before startDate and not after today
          min={startDate || undefined}
          max={maxDate}
          onChange={(event) => onEndDateChange(event.target.value)}
        />
      </label>
    </div>
  );
}
