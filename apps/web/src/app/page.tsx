import Link from "next/link";

import styles from "./page.module.css";

export default function Home() {
  return (
    <main className={styles.page}>
      <section className={styles.card}>
        <h1>Diamond Insights Web</h1>
        <p>Web app foundation for user-facing features and internal admin tooling.</p>
        <div className={styles.actions}>
          <Link href="/admin">Open Admin Portal</Link>
        </div>
      </section>
    </main>
  );
}
