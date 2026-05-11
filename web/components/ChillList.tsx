'use client';

import { useEffect, useState } from 'react';

type Item = {
  biz_reg_no: number;
  name: string;
  category: string;
  district: string;
  latitude: number;
  longitude: number;
  open_hour: number;
  close_hour: number;
  avg_congest_score: number;
  is_open_now: boolean;
};

export default function ChillList() {
  const [items, setItems] = useState<Item[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    const base = process.env.NEXT_PUBLIC_API_BASE!;
    fetch(`${base}/api/chill-open`)
      .then((r) => r.json())
      .then((d) => setItems(d.items ?? []))
      .catch((e) => setError(String(e)))
      .finally(() => setLoaded(true));
  }, []);

  if (error) return <div className="p-4 text-red-400">API 오류: {error}</div>;
  if (loaded && items.length === 0)
    return <div className="p-4 text-zinc-400">조건 충족 가게 없음.</div>;

  return (
    <ul className="divide-y divide-zinc-800">
      {items.map((i) => (
        <li
          key={i.biz_reg_no}
          className="px-4 py-3 flex items-center justify-between"
        >
          <div>
            <div className="text-base font-medium">{i.name}</div>
            <div className="text-xs text-zinc-400">
              {i.district} &middot; {i.category} &middot; {i.open_hour}~
              {i.close_hour}시
            </div>
          </div>
          <div className="text-xs">
            <span className="px-2 py-1 rounded bg-emerald-900/40 text-emerald-300">
              한가 {i.avg_congest_score?.toFixed(2)}
            </span>
          </div>
        </li>
      ))}
    </ul>
  );
}
