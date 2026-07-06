type ListSkeletonProps = {
  count?: number;
  label?: string;
  variant?: "compact" | "card";
};

export function ListSkeleton({ count = 5, label = "Loading items...", variant = "compact" }: ListSkeletonProps) {
  return (
    <div className={`list-skeleton list-skeleton--${variant}`} aria-busy="true" aria-label={label}>
      {Array.from({ length: count }, (_, index) => (
        <div className="list-skeleton__item" key={index}>
          <span className="table-loading-cell table-loading-cell--wide" />
          <span className="table-loading-cell" />
          {variant === "card" ? <span className="table-loading-cell table-loading-cell--short" /> : null}
        </div>
      ))}
    </div>
  );
}
