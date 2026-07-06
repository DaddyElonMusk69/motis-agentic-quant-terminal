type DetailSkeletonProps = {
  fields?: string[];
  label?: string;
  rows?: number;
};

export function DetailSkeleton({ fields, label = "Loading detail...", rows = 6 }: DetailSkeletonProps) {
  const labels = fields ?? Array.from({ length: rows }, (_, index) => `Field ${index + 1}`);
  return (
    <div className="detail-skeleton" aria-busy="true" aria-label={label}>
      <div className="field-grid">
        {labels.map((field) => (
          <div className="field-row field-row--loading" key={field}>
            <span>{field}</span>
            <span className="table-loading-cell" />
          </div>
        ))}
      </div>
    </div>
  );
}
