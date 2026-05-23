import type { Metadata } from "next";
import "./globals.css";
import "leaflet/dist/leaflet.css";

export const metadata: Metadata = {
  title: "seoulnow — 서울 실시간 혼잡도 + 한가한 카페·술집",
  description:
    "지금 한가한 동네와 영업 중인 카페·술집을 5분 주기로 갱신해서 보여줍니다.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
