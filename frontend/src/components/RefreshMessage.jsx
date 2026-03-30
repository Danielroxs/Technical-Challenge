import { useEffect, useState } from "react";

export default function RefreshMessage({ message, variant = "success" }) {
  const [visible, setVisible] = useState(true);

  useEffect(() => {
    const timeoutId = setTimeout(() => {
      setVisible(false);
    }, 5000);

    return () => clearTimeout(timeoutId);
  }, []);

  if (!message || !visible) return null;

  return <p className={`refresh-message ${variant}`}>{message}</p>;
}
