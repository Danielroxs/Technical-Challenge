import { useEffect, useState } from "react";

export default function RefreshMessage({ message, variant = "success" }) {
  // Local visibility state allows the message to disappear automatically
  const [visible, setVisible] = useState(true);

  useEffect(() => {
    // Auto-hide feedback message after a few seconds
    const timeoutId = setTimeout(() => {
      setVisible(false);
    }, 5000);

    return () => clearTimeout(timeoutId);
  }, []);

  if (!message || !visible) return null;

  return <p className={`refresh-message ${variant}`}>{message}</p>;
}
