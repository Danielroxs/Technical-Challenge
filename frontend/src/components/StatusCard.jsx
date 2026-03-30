export default function StatusCard({
  title,
  message,
  variant = "default",
  showSpinner = false,
}) {
  return (
    <div className={`status-card ${variant}`}>
      {showSpinner && <div className="status-spinner" aria-hidden="true" />}

      <div className="status-card-content">
        <h2>{title}</h2>
        <p>{message}</p>
      </div>
    </div>
  );
}
