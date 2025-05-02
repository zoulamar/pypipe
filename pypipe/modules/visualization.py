
class Plot (BaseModule):
    def __init__(self, module_path: Path) -> None:
        super().__init__(module_path)
        assert self.parent_module is not None

    def declare_targets(self) -> dict[str, GenericDataType]:
        config_file = YamlDataType(self.module_path / "config.yaml", GenericDataType.expect_made)
        ret:dict[str, GenericDataType] = {}

        # Understand npy files
        for t_lab, t_obj in GenericDataType.filteritems(self.parent_module.targets,NpzDataType):
            ret[t_lab] = GenericDataType(self.module_path / t_obj.path.with_suffix(".pdf").name, Plot.npz, depends={"src":t_obj,"cfg":config_file})

        return ret

    @staticmethod
    def npz(target:GenericDataType):
        cfg = target.depends["cfg"].get()
        src = target.depends["src"].get()

        if "t" in src:
            t = src["t"]
            t = t - t[0]
            del src["t"]
        elif "time" in src:
            t = src["time"]
            t = t - t[0]
            del src["time"]
        else:
            t = None

        try:
            with PdfPages(target.path) as pdf:
                for data_name, data in src.items():
                    fig = plt.figure(figsize=(IEEE_COL_WIDTH, IEEE_COL_WIDTH*.8))
                    if t is None:
                        plt.plot(data)
                        plt.xlabel("Sample [-]")
                    else:
                        plt.plot(t, data)
                        plt.xlabel("Time [s]")
                    plt.ylabel(data_name)
                    pdf.savefig()
                    plt.close()
        except:
            os.remove(target.path)
            raise

