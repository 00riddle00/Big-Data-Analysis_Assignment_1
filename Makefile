PRES_DIR     := presentation
PRESENTATION := $(PRES_DIR)/presentation_1st.pdf
LATEXMK      := latexmk

# --- Presentation ------------------------------------------------------------
presentation: $(PRESENTATION)

$(PRESENTATION): $(PRES_DIR)/presentation_1st.tex
	cd $(PRES_DIR) && $(LATEXMK) -xelatex -interaction=nonstopmode presentation_1st.tex
	@echo "Presentation compiled: $(PRESENTATION)"

# --- Clean -------------------------------------------------------------------
clean:
	rm -f $(PRES_DIR)/*.aux $(PRES_DIR)/*.log $(PRES_DIR)/*.nav
	rm -f $(PRES_DIR)/*.out $(PRES_DIR)/*.snm $(PRES_DIR)/*.toc
	rm -f $(PRES_DIR)/*.fls $(PRES_DIR)/*.fdb_latexmk $(PRES_DIR)/*.xdv
	rm -f $(PRESENTATION)
	@echo "Cleaned presentation build files."
