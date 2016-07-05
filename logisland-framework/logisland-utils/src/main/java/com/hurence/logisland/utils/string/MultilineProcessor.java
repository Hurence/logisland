package com.hurence.logisland.utils.string;


import com.sun.tools.javac.model.JavacElements;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import java.util.Set;


/**
 * credits to http://www.adrianwalker.org
 */
@SupportedAnnotationTypes({"com.hurence.logisland.utils.string.Multiline"})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public final class MultilineProcessor extends AbstractProcessor {

    private JavacElements elementUtils;
    private TreeMaker maker;

    @Override
    public void init(final ProcessingEnvironment procEnv) {
        super.init(procEnv);
        JavacProcessingEnvironment javacProcessingEnv = (JavacProcessingEnvironment) procEnv;
        this.elementUtils = javacProcessingEnv.getElementUtils();
        this.maker = TreeMaker.instance(javacProcessingEnv.getContext());
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {

        Set<? extends Element> fields = roundEnv.getElementsAnnotatedWith(Multiline.class);
        for (Element field : fields) {
            String docComment = elementUtils.getDocComment(field);
            if (null != docComment) {
                JCTree.JCVariableDecl fieldNode = (JCTree.JCVariableDecl) elementUtils.getTree(field);
                fieldNode.init = maker.Literal(docComment);
            }
        }

        return true;
    }
}