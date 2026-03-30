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
          max={endDate || maxDate}
          onChange={(event) => onStartDateChange(event.target.value)}
        />
      </label>

      <label className="field-group">
        <span>End date</span>
        <input
          type="date"
          value={endDate}
          min={startDate || undefined}
          max={maxDate}
          onChange={(event) => onEndDateChange(event.target.value)}
        />
      </label>
    </div>
  );
}
