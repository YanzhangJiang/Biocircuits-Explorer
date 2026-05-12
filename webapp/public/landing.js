(() => {
  "use strict";

  const header = document.querySelector(".site-header");
  if (header) {
    const update = () => {
      header.dataset.scroll = window.scrollY > 12 ? "true" : "false";
    };
    update();
    window.addEventListener("scroll", update, { passive: true });
  }

  document.querySelectorAll("video[autoplay]").forEach((video) => {
    video.muted = true;
    const tryPlay = () => {
      const result = video.play();
      if (result && typeof result.catch === "function") {
        result.catch(() => {});
      }
    };
    tryPlay();
    video.addEventListener("loadeddata", tryPlay, { once: true });
  });
})();
