;; .emacs

;;; uncomment this line to disable loading of "default.el" at startup
;; (setq inhibit-default-init t)

(setq compilation-scroll-output 'first-error)
;;(setq split-width-threshold  (ceiling (frame-width) 2))
;;(setq split-height-threshold nil)
;;(setq display-buffer-reuse-frames t)

;; turn on font-lock mode
(when (fboundp 'global-font-lock-mode)
  (global-font-lock-mode t))

;; enable visual feedback on selections
;(setq transient-mark-mode t)

;; default to better frame titles
(setq frame-title-format
      (concat  "%b - emacs@" (system-name)))

;; default to unified diffs
(setq diff-switches "-u")

;; always end a file with a newline
;(setq require-final-newline 'query)

(setq viper-mode t)
(require 'viper)

;; show *compilation* in inactive buffer
(defun display-on-side (buffer &optional not-this-window frame)
  (let* ((window (or (minibuffer-selected-window)
                     (selected-window)))
         (display-buffer-function nil)
         (pop-up-windows nil))
    (with-selected-window (or window (error "display-on-side"))
      (when (one-window-p t)
        (split-window-horizontally))
      (display-buffer buffer not-this-window frame))))
(setq display-buffer-function 'display-on-side)

(setq case-fold-search t)   ; make searches case insensitive

;; indentation-related
(setq-default viper-auto-indent t)
(setq-default indent-tabs-mode nil)
(setq-default c-basic-offset 2)
(setq-default c-syntactic-indentation nil)
